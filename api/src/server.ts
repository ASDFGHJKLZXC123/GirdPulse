import { ApolloServer } from '@apollo/server';
import { ApolloServerPluginDrainHttpServer } from '@apollo/server/plugin/drainHttpServer';
import { expressMiddleware } from '@apollo/server/express4';
import bodyParser from 'body-parser';
import cors from 'cors';
import express from 'express';
import { createServer, type Server } from 'node:http';
import { type AddressInfo } from 'node:net';
import { Pool } from 'pg';
import { useServer } from 'graphql-ws/use/ws';
import { WebSocketServer } from 'ws';
import type { ApiConfig } from './config.js';
import { EventBus } from './event-bus.js';
import { KafkaTail } from './kafka-tail.js';
import { createRequestContext, type ApiContext } from './resolvers.js';
import { createApiSchema } from './schema.js';

export interface SubscriptionTail {
  readonly groupId: string;
  start(): Promise<void>;
  stop(): Promise<void>;
}

export interface StartApiOptions {
  config: ApiConfig;
  pool?: Pool;
  eventBus?: EventBus;
  tail?: SubscriptionTail;
}

export interface RunningApi {
  readonly groupId: string;
  readonly httpServer: Server;
  readonly port: number;
  readonly url: string;
  stop(): Promise<void>;
}

function listen(httpServer: Server, port: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error): void => {
      httpServer.off('listening', onListening);
      reject(error);
    };
    const onListening = (): void => {
      httpServer.off('error', onError);
      const address = httpServer.address();
      if (!address || typeof address === 'string') {
        reject(new Error('API server did not expose a TCP address'));
        return;
      }
      resolve((address as AddressInfo).port);
    };
    httpServer.once('error', onError);
    httpServer.once('listening', onListening);
    httpServer.listen(port);
  });
}

export async function startApi(options: StartApiOptions): Promise<RunningApi> {
  const { config } = options;
  const pool = options.pool ?? new Pool({ connectionString: config.databaseUrl });
  const ownsPool = options.pool === undefined;
  const eventBus = options.eventBus ?? new EventBus();
  const tail =
    options.tail ??
    new KafkaTail(
      {
        brokers: config.brokers,
        schemaRegistryUrl: config.schemaRegistryUrl,
      },
      eventBus,
    );
  const schema = createApiSchema();
  const app = express();
  const httpServer = createServer(app);
  const webSocketServer = new WebSocketServer({
    server: httpServer,
    path: '/graphql',
  });
  const websocketCleanup = useServer(
    {
      schema,
      context: () => createRequestContext(pool, eventBus),
    },
    webSocketServer,
  );
  const apollo = new ApolloServer<ApiContext>({
    schema,
    plugins: [
      ApolloServerPluginDrainHttpServer({ httpServer }),
      {
        async serverWillStart() {
          return {
            async drainServer() {
              await websocketCleanup.dispose();
            },
          };
        },
      },
    ],
  });

  let apolloStarted = false;
  let tailStarted = false;
  let stopPromise: Promise<void> | undefined;

  const stopOnce = async (): Promise<void> => {
    const errors: unknown[] = [];
    if (tailStarted) {
      await tail.stop().catch((error: unknown) => errors.push(error));
    }
    if (apolloStarted) {
      await apollo.stop().catch((error: unknown) => errors.push(error));
    } else {
      await Promise.resolve(websocketCleanup.dispose()).catch((error: unknown) =>
        errors.push(error),
      );
      webSocketServer.close();
      httpServer.close();
    }
    if (ownsPool) {
      await pool.end().catch((error: unknown) => errors.push(error));
    }

    if (errors.length > 0) {
      throw new AggregateError(errors, 'API shutdown failed');
    }
  };
  const stop = (): Promise<void> => {
    stopPromise ??= stopOnce();
    return stopPromise;
  };

  try {
    await pool.query('SELECT 1');
    await apollo.start();
    apolloStarted = true;

    app.use(
      '/graphql',
      cors<cors.CorsRequest>(),
      bodyParser.json(),
      expressMiddleware(apollo, {
        context: async () => createRequestContext(pool, eventBus),
      }),
    );

    await tail.start();
    tailStarted = true;
    const port = await listen(httpServer, config.port);

    return {
      groupId: tail.groupId,
      httpServer,
      port,
      url: `http://localhost:${port}/graphql`,
      stop,
    };
  } catch (error) {
    await stop().catch(() => undefined);
    throw error;
  }
}
