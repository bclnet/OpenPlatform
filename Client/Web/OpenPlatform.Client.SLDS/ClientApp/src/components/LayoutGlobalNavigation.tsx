import * as React from 'react';
import { useRouteMatch } from 'react-router-dom';
import {
    GlobalNavigationBar, GlobalNavigationBarRegion, GlobalNavigationBarLink,
    AppLauncher, AppLauncherExpandableSection, AppLauncherTile,
    Button
} from '@salesforce/design-system-react';

export function LayoutGlobalNavigation() {
    const l_home: boolean = false; //useRouteMatch('/')?.isExact || false;
    const l_path2: boolean = false; //useRouteMatch('/path2')?.isExact || false;
    return (
        <GlobalNavigationBar>
            <GlobalNavigationBarRegion region="primary">
                <AppLauncher id="app-launcher-trigger"
                    triggerName="TriggerName"
                    onSearch={(event: any) => {
                        console.log('Search term:', event.target.value);
                    }}
                    modalHeaderButton={<Button label="Open" />}
                >
                    <AppLauncherExpandableSection title="Tile Section">
                        <AppLauncherTile
                            title="Open"
                            iconText="MC"
                            description="Navigate to Open"
                        />
                        <AppLauncherTile
                            title="Open2"
                            iconText="MC"
                            description="Navigate to Open2"
                        />
                    </AppLauncherExpandableSection>
                </AppLauncher>
            </GlobalNavigationBarRegion>
            <GlobalNavigationBarRegion region="secondary" navigation>
                <GlobalNavigationBarLink active={l_home} href={'/home'} label="Home" id="home-link" />
                <GlobalNavigationBarLink active={l_path2} href={'/path2'} label="Path2" />
            </GlobalNavigationBarRegion>
        </GlobalNavigationBar>
    );
}
