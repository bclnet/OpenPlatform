Web - Platform Client
===============

TBD

---

* [Material-UI](https://material-ui.com)
* [Lightning Design System for React](https://react.lightningdesignsystem.com)

* IObjectMgr
* IAppMgr
* AppController
* SchemaController

* Query LocalStore for header.schemaId
* All calls include header.schemaId
* HttpCode:418 if client and server schema is different
* Sync LocalStore on HttpCode:418
* Use `[Attribute]` on server controller

* LocalStore
  * Header
  * AppList (unit)
  * Object Schema (unit)
