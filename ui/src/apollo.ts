import { ApolloClient, HttpLink, InMemoryCache, split } from '@apollo/client';
import { GraphQLWsLink } from '@apollo/client/link/subscriptions';
import { getMainDefinition } from '@apollo/client/utilities';
import { createClient } from 'graphql-ws';

const graphqlHttpUrl = import.meta.env.VITE_GRAPHQL_HTTP || 'http://localhost:4000/graphql';
const graphqlWsUrl = import.meta.env.VITE_GRAPHQL_WS || 'ws://localhost:4000/graphql';

const httpLink = new HttpLink({
  uri: graphqlHttpUrl,
});

const subscriptionLink = new GraphQLWsLink(
  createClient({
    url: graphqlWsUrl,
    lazy: true,
    retryAttempts: Number.POSITIVE_INFINITY,
  }),
);

const link = split(
  ({ query }) => {
    const definition = getMainDefinition(query);
    return definition.kind === 'OperationDefinition' && definition.operation === 'subscription';
  },
  subscriptionLink,
  httpLink,
);

export const apolloClient = new ApolloClient({
  link,
  cache: new InMemoryCache(),
});
