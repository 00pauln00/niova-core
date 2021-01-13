import express from 'express';
import { ApolloServer } from 'apollo-server-express';
import { typeDefs } from './schema';
import { getConfig } from '../utils/config';
import { cmdJson } from './niova-api';

const { SERVER_PORT } = getConfig();

interface CmdPair {
    uuid: string;
    path: string;
}

const resolvers = {
    Query: {
        getJson: (_parent: any, { uuid, path }: CmdPair) =>
            cmdJson('GET', uuid, path),
    },
};

const app = express();
const server = new ApolloServer({ typeDefs, resolvers });
server.applyMiddleware({ app });

app.use((_req, res) => {
    res.status(200);
    res.send('Hello!');
    res.end();
});

app.listen({ port: SERVER_PORT }, () =>
    console.log(
        `Server ready at http://localhost:${SERVER_PORT}${server.graphqlPath}`
    )
);
