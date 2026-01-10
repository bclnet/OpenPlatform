import * as React from 'react';
import {
    Settings, IconSettings,
    BrandBand
} from '@salesforce/design-system-react';
import { LayoutGlobalHeader } from './LayoutGlobalHeader';

export default class Layout extends React.PureComponent<{}, { children?: React.ReactNode }> {
    public render() {
        //Settings.setAppElement('#root');
        return (
            <React.Fragment>
                <IconSettings iconPath="/assets/icons">
                    <LayoutGlobalHeader />
                    <BrandBand id="brand-band" className="slds-p-around_small">
                        {this.props.children}
                    </BrandBand>
                </IconSettings>
            </React.Fragment>
        );
    }
}
