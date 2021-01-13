import { Button, Card, H5, Classes, FormGroup } from '@blueprintjs/core';
import { ReactElement } from 'react';

interface DataFilterOpts {
    uuid: string;
    path: string;
    setUuid(uuid: string): void;
    setPath(path: string): void;
    onSearch(): void;
}

export default function DataFilter({
    uuid,
    setUuid,
    path,
    setPath,
    onSearch,
}: DataFilterOpts): ReactElement | null {
    return (
        <Card>
            <H5>NIOVA Config Filters</H5>
            <FormGroup label="Service UUID">
                <input
                    className={Classes.INPUT}
                    value={uuid}
                    onChange={(event: any) => setUuid(event?.target?.value)}
                />
            </FormGroup>
            <FormGroup label="Data Object Path">
                <input
                    className={Classes.INPUT}
                    value={path}
                    onChange={(event: any) => setPath(event?.target?.value)}
                />
            </FormGroup>
            <Button onClick={onSearch}>Update</Button>
        </Card>
    );
}
