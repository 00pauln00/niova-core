import { ApolloError } from '@apollo/client';
import { ReactElement } from 'react';
import ReactJson, { InteractionProps, OnSelectProps } from 'react-json-view';

interface DataViewProps {
    data: any;
    loading: boolean;
    error?: ApolloError;
    onKeySelect(props: OnSelectProps): void;
    onEdit?(props: InteractionProps): void;
}

export default function DataView({
    loading,
    error,
    data,
    onKeySelect,
    onEdit,
}: DataViewProps): ReactElement | null {
    if (loading) {
        return <div>Loading...</div>;
    } else if (error) {
        return <div>Error! {error.toString()}</div>;
    } else if (data) {
        return (
            <ReactJson
                displayDataTypes={false}
                src={data}
                onKeySelect={onKeySelect}
                onEdit={onEdit}
            />
        );
    } else {
        return null;
    }
}
