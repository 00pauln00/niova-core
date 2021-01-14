import { gql, useQuery } from '@apollo/client';
import { ReactElement } from 'react';

const GET_JSON_QUERY = gql`
    query GetJson($uuid: String!, $path: String!) {
        getJson(uuid: $uuid, path: $path) {
            json
        }
    }
`;

export interface SearchParams {
    uuid: string;
    path: string;
    useCache: boolean;
}

interface GetJsonOpts extends SearchParams {
    render({ data }: {data: any}) : ReactElement | null;
}

function addWildCard(path: string) {
    if (path.includes('.*')) {
        return path;
    }

    let new_path = path;
    if (path[path.length - 1] != '/') {
        new_path += '/';
    }

    return new_path + '/.*/.*';
}

export default function GetJson({
    uuid,
    path,
    useCache,
    render,
}: GetJsonOpts): ReactElement | null {
    const { loading, error, data } = useQuery(GET_JSON_QUERY, {
        fetchPolicy: useCache ? 'cache-first' : 'cache-and-network',
        variables: { uuid, path: addWildCard(path) },
    });

    if (loading) {
        return <div>Loading...</div>; // XXX add some UI
    }
    if (error) {
        return <div>Error! {error}</div>;
    }

    try {
        const obj = JSON.parse(data.getJson?.json);

        return render({ data: obj });
    } catch (e) {
        return <div>Error parsing JSON: {JSON.stringify(data, null, 4)} </div>;
    }
}
