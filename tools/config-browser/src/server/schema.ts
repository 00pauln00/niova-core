import { gql } from 'apollo-server-express';

export const typeDefs = gql`
    type TreeFragJson {
        id: String
        json: String
    }

    type Service {
        uuid: String
        uptime: Int
        pid: Int
    }

    type Query {
        getJson(
            "NIOVA service UUID"
            uuid: String!
            "NIOVA JSON data location"
            path: String!
        ): TreeFragJson
        getServices: [Service]
    }

    type Mutation {
        applyJson(
            "NIOVA service UUID"
            uuid: String!
            "NIOVA JSON data location"
            path: String!
            value: String!
        ): TreeFragJson
    }
`;
