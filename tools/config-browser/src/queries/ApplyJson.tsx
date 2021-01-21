import { gql, useMutation } from '@apollo/client';
import { ReactElement } from 'react';
import { DataLoadingError } from '../types';

const APPLY_JSON_MUTATION = gql`
    mutation ApplyJson($uuid: String!, $path: String!, $value: String!) {
        applyJson(uuid: $uuid, path: $path, value: $value) {
            id
            json
        }
    }
`;

export interface ApplyParams {
    uuid: string;
    path: string;
    value: string;
}

interface ApplyJsonOpts extends ApplyParams {
    render({ data, loading, error }: DataLoadingError): ReactElement | null;
}

export default function ApplyJson({
    uuid,
    path,
    value,
    render,
}: ApplyJsonOpts): ReactElement | null {
    const [applyJson, { data, loading, error }] = useMutation(APPLY_JSON_MUTATION);
    applyJson({ variables: { uuid, path, value } });

    return render({ data, loading, error });
}
