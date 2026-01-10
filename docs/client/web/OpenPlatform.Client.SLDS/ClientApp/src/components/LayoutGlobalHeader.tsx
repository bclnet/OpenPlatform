import * as React from 'react';
import {
    GlobalHeader, GlobalHeaderSearch, GlobalHeaderHelp, GlobalHeaderNotifications, GlobalHeaderProfile,
    Combobox, Popover
} from '@salesforce/design-system-react';
import { LayoutGlobalNavigation } from './LayoutGlobalNavigation';

const ipsum = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec bibendum fermentum eros, vel porta metus dignissim vitae. Fusce finibus sed magna vitae tempus. Suspendisse condimentum, arcu eu viverra vulputate, mauris odio dictum velit, in dictum lorem augue id augue. Proin nec leo convallis, aliquet mi ut, interdum nunc.';

const HeaderNotificationsCustomContent = (props: any) => (
    <ul id="header-notifications-custom-popover-content">
        {props.items.map((item: any) => (
            <li className={`slds-global-header__notification ${item.unread ? 'slds-global-header__notification_unread' : ''}`} key={`notification-item-${item.id}`} >
                <div className="slds-media slds-has-flexi-truncate slds-p-around_x-small">
                    <div className="slds-media__figure">
                        <span className="slds-avatar slds-avatar_small">
                            <img
                                alt={item.name}
                                src={`/assets/images/${item.avatar}.jpg`}
                                title={`${item.name} avatar"`}
                            />
                        </span>
                    </div>
                    <div className="slds-media__body">
                        <div className="slds-grid slds-grid_align-spread">
                            <a href="#" className="slds-text-link_reset slds-has-flexi-truncate" >
                                <h3 className="slds-truncate" title={`${item.name} ${item.action}`} >
                                    <strong>{`${item.name} ${item.action}`}</strong>
                                </h3>
                                <p className="slds-truncate" title={item.comment}>
                                    {item.comment}
                                </p>
                                <p className="slds-m-top_x-small slds-text-color_weak">
                                    {item.timePosted}{' '}
                                    {item.unread ? (<abbr className="slds-text-link slds-m-horizontal_xxx-small" title="unread" >●</abbr>) : null}
                                </p>
                            </a>
                        </div>
                    </div>
                </div>
            </li>
        ))}
    </ul>
);
HeaderNotificationsCustomContent.displayName = 'HeaderNotificationsCustomContent';

const HeaderProfileCustomContent = (props: any) => (
    <div id="header-profile-custom-popover-content">
        <div className="slds-m-around_medium">
            <div className="slds-tile slds-tile_board slds-m-horizontal_small">
                <p className="tile__title slds-text-heading_small">Art Vandelay</p>
                <div className="slds-tile__detail">
                    <p className="slds-truncate">
                        <a className="slds-m-right_medium" href="javascript:void(0)" onClick={props.onClick}>
                            Settings
              </a>
                        <a href="javascript:void(0)" onClick={props.onClick}>
                            Log Out
              </a>
                    </p>
                </div>
            </div>
        </div>
    </div>
);
HeaderProfileCustomContent.displayName = 'HeaderProfileCustomContent';

export function LayoutGlobalHeader() {
    return (
        <GlobalHeader
            logoSrc="/assets/images/logo.svg"
            onSkipToContent={() => {
                console.log('>>> Skip to Content Clicked');
            }}
            onSkipToNav={() => {
                console.log('>>> Skip to Nav Clicked');
            }}
            navigation={<LayoutGlobalNavigation />}
        >
            <GlobalHeaderSearch
                combobox={
                    <Combobox
                        assistiveText={{ label: 'Search' }}
                        events={{
                            onSelect: () => {
                                console.log('>>> onSelect');
                            },
                        }}
                        id="header-search-custom-id"
                        labels={{ placeholder: 'Search Salesforce' }}
                        options={[
                            { id: 'email', label: 'Email' },
                            { id: 'mobile', label: 'Mobile' },
                        ]}
                    />
                }
            />
            <GlobalHeaderHelp
                popover={
                    <Popover id="header-help-popover-id"
                        ariaLabelledby="help-heading"
                        body={
                            <div>
                                <h2 className="slds-text-heading_small" id="help-heading">
                                    Help and Training
                </h2>
                                {ipsum}
                            </div>
                        }
                    />
                }
            />
            <GlobalHeaderNotifications
                notificationCount={0}
                popover={
                    <Popover
                        ariaLabelledby="header-notifications-custom-popover-content"
                        body={
                            <HeaderNotificationsCustomContent id="header-notifications-popover-id"
                                items={[
                                    {
                                        action: 'mentioned you',
                                        avatar: 'avatar2',
                                        comment: '@jrogers Could I please have a review on my presentation deck',
                                        id: 1,
                                        name: 'Val Handerly',
                                        timePosted: '10 hours ago',
                                        unread: true,
                                    },
                                    {
                                        action: 'commented on your post',
                                        avatar: 'avatar3',
                                        comment: 'I totally agree with your sentiment',
                                        id: 2,
                                        name: 'Jon Rogers',
                                        timePosted: '13 hours ago',
                                        unread: true,
                                    },
                                    {
                                        action: 'mentioned you',
                                        avatar: 'avatar2',
                                        comment: "@jrogers Here's the conversation I mentioned to you",
                                        id: 3,
                                        name: 'Rebecca Stone',
                                        timePosted: '1 day ago',
                                    },
                                ]}
                            />
                        }
                    />
                }
            />
            <GlobalHeaderProfile
                popover={<Popover body={<HeaderProfileCustomContent />} id="header-profile-popover-id" />}
                userName="My User"
            />
        </GlobalHeader>
    );
}
