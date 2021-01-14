import { gql } from 'apollo-server-express';

export const typeDefs = gql`
type Query {
    getJson("NIOVA service UUID" uuid: String!, "NIOVA JSON data location" path: String!):  String
    applyJson("NIOVA service UUID" uuid: String!,
              "NIOVA JSON data location" path: String!,
              value: String!):  String
}`;
