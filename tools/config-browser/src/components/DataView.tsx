import { ReactElement } from 'react';
import ReactJson from 'react-json-view';
import { OnSelectProps } from 'react-json-view';

interface DataViewProps {
    data: any;
    onKeySelect(props: OnSelectProps): void;
}

export default function getDataView({ data, onKeySelect }: DataViewProps): ReactElement {
    // filter out array indexes
    return <ReactJson displayDataTypes={false} src={data} onKeySelect={onKeySelect} />;
}
