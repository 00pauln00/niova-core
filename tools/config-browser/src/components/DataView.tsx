import { ReactElement } from 'react';
import ReactJson from 'react-json-view';
import { GetJsonRenderProps } from './GetJson';

export default function DataView({ data, onKeySelect }: GetJsonRenderProps): ReactElement {
    return <ReactJson displayDataTypes={false} src={data} onKeySelect={onKeySelect} />;
}
