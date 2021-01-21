import { ReactElement } from 'react';
import ReactJson, { InteractionProps, OnSelectProps } from 'react-json-view';

interface DataViewProps {
    data: any;
    onKeySelect(props: OnSelectProps): void;
    onEdit(props: InteractionProps): void;
}

export default function renderDataView({ data, onKeySelect, onEdit }: DataViewProps): ReactElement {
    // filter out array indexes
    return (
        <ReactJson displayDataTypes={false} src={data} onKeySelect={onKeySelect} onEdit={onEdit} />
    );
}
