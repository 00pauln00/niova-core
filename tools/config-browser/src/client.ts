import { ApolloClient, InMemoryCache } from '@apollo/client';
import { getConfig } from './utils/config';

const { CLIENT_URI } = getConfig();

export const newClient = () =>
    new ApolloClient({
        uri: CLIENT_URI,
        cache: new InMemoryCache(),
    });
