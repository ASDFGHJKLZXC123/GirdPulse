import { loadConfig } from './config.js';
import { startApi } from './server.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const api = await startApi({ config });
  console.log(`api subscription consumer joined groupId=${api.groupId}`);
  console.log(`api ready url=${api.url}`);

  let shuttingDown = false;
  const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    console.log(`api stopping signal=${signal}`);
    try {
      await api.stop();
      console.log('api stopped');
    } catch (error) {
      console.error(
        `api shutdown failed: ${error instanceof Error ? error.message : String(error)}`,
      );
      process.exitCode = 1;
    }
  };

  process.once('SIGINT', () => void shutdown('SIGINT'));
  process.once('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((error: unknown) => {
  console.error(`api startup failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
