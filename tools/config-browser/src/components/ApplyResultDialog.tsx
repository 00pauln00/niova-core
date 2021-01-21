import { Dialog } from '@blueprintjs/core';
import { ReactElement } from 'react';

interface DataViewProps {
    data: any;
    loading: boolean;
    error: any;
    onClose(): void;
}

export default function renderApplyResultDialog({
    data,
    loading,
    error,
    onClose,
}: DataViewProps): ReactElement {
    // filter out array indexes
    return (
        <Dialog isOpen={true} onClose={onClose}>
            {loading && <div>Loading...</div>}
            {error && 'Got error!' + error}
            Data: {JSON.stringify(data, null, 4)}
        </Dialog>
    );
}
