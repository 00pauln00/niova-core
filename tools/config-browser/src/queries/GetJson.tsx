import { gql, useQuery } from '@apollo/client';
import { ReactElement } from 'react';
import { OnSelectProps } from 'react-json-view';

const GET_JSON_QUERY = gql`
    query GetJson($uuid: String!, $path: String!) {
        getJson(uuid: $uuid, path: $path)
    }
`;

export interface GetJsonRenderProps {
    data: any;
    onKeySelect(props: OnSelectProps): void;
}

export interface SearchParams {
    uuid: string;
    path: string;
    useCache: boolean;
}

interface GetJsonOpts extends SearchParams {
    setPath(path: string): void;
    render({ data, onKeySelect }: GetJsonRenderProps): ReactElement | null;
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

const isNumeric = (num: any): boolean =>
    typeof num == 'number' || (typeof num == 'string' && !isNaN(+num) && !isNaN(parseFloat(num)));

export default function GetJson({
    uuid,
    path,
    useCache,
    render,
    setPath,
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

    // filter out array indexes
    const onKeySelect = ({ namespace }: OnSelectProps) =>
        setPath('/' + namespace.filter((o) => !isNumeric(o)).join('/'));

    try {
        const obj = JSON.parse(data.getJson);

        return render({ data: obj, onKeySelect });
    } catch (e) {
        return <div>Error parsing JSON: {JSON.stringify(data, null, 4)} </div>;
    }
}
