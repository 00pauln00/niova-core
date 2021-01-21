import { gql } from '@apollo/client';

export const DEFAULT_UUID = '';
export const DEFAULT_PATH = '/';

export const GET_SERVICES_QUERY = gql`
    query GetServices {
        getServices {
            uuid
            uptime
            pid
        }
    }
`;

export const GET_JSON_QUERY = gql`
    query GetJson($uuid: String!, $path: String!) {
        getJson(uuid: $uuid, path: $path) {
            json
        }
    }
`;

export const APPLY_JSON_MUTATION = gql`
    mutation ApplyJson($uuid: String!, $path: String!, $value: String!) {
        applyJson(uuid: $uuid, path: $path, value: $value) {
            id
            json
        }
    }
`;
