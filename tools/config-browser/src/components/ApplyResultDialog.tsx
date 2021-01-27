import { Dialog } from '@blueprintjs/core';
import { ReactElement } from 'react';

interface Props {
    data?: any;
    loading: boolean;
    error?: any;
    onClose(): void;
}

export default function ApplyResultDialog({
    data,
    loading,
    error,
    onClose,
}: Props): ReactElement | null {
    let msg = '';
    if (!loading && !error) {
        const respJson = data?.applyJson?.json;
        const id = data?.applyJson?.id;

        try {
            const resp = JSON.parse(respJson);
            if (typeof resp == 'object' && Object.keys(resp).length == 0) {
                msg = `${id}: saved`;
            } else {
                msg = `${id}: error: ${respJson}`;
            }
        } catch (e) {
            msg = `${id}: error parsing JSON: ${respJson}`;
        }
    }

    // filter out array indexes
    return (
        <Dialog isOpen={true} onClose={onClose}>
            {loading && <div>Loading...</div>}
            {error && 'Got error!' + error}
            {msg}
        </Dialog>
    );
}
