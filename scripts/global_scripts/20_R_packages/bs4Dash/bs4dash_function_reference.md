# bs4Dash Function Reference

This document provides a comprehensive reference of all permissible functions in the bs4Dash package, with a focus on the newer and more specific function names. Functions are organized by category for easy reference.

## Core Page Structure

### Page Container
- `bs4DashPage` - Main dashboard page container (preferred over alias `dashboardPage`)
  - Parameters: header, sidebar, body, controlbar, footer, title, skin, freshTheme, preloader, options, fullscreen, help, dark, scrollToTop

### Header 
- `bs4DashNavbar` - Dashboard header/navbar (preferred over alias `dashboardHeader`)
  - Parameters: title, titleWidth, disable, leftUi, rightUi, skin, status, border, compact, sidebarIcon, controlbarIcon, fixed

### Sidebar
- `bs4DashSidebar` - Dashboard sidebar (preferred over alias `dashboardSidebar`) 
  - Parameters: disable, width, skin, status, elevation, collapsed, minified, expandOnHover, fixed, id, customArea

### Body
- `bs4DashBody` - Dashboard body/main content area (preferred over alias `dashboardBody`)

### Controlbar
- `bs4DashControlbar` - Right-side control bar (preferred over alias `dashboardControlbar`)

### Footer
- `bs4DashFooter` - Dashboard footer (preferred over alias `dashboardFooter`)

## Navigation & Menu Components

### Sidebar Navigation
- `bs4SidebarMenu` - Sidebar menu container (preferred over alias `sidebarMenu`)
  - Parameters: id, .list, flat, compact, childIndent, legacy

- `bs4SidebarMenuproduct` - Main sidebar menu product (preferred over alias `menuproduct`)
  - Parameters: text, icon, badgeLabel, badgeColor, tabName, href, newTab, selected, expandedName, startExpanded, condition, .list

- `bs4SidebarMenuSubproduct` - Sidebar submenu product (preferred over alias `menuSubproduct`) 
  - Parameters: text, tabName, href, newTab, icon, selected

- `bs4SidebarHeader` - Sidebar section header (preferred over alias `sidebarHeader`)
  - Parameters: title

- `bs4SidebarUserPanel` - Sidebar user info panel (preferred over alias `sidebarUserPanel`)
  - Parameters: name, image

### Navbar Navigation
- `navbarMenu` - Navigation menu in the navbar
  - Parameters: id, ...

- `navbarTab` - Tab product in the navbar menu
  - Parameters: text, tabName, icon, .list

### Tab Navigation
- `bs4Tabproducts` - Container for tab content products (preferred over alias `tabproducts`)

- `bs4Tabproduct` - Individual tab content (preferred over alias `tabproduct`)
  - Parameters: tabName, ...

- `bs4TabCard` - Card with tabs (preferred over alias `tabBox`)

## Cards & Boxes

### Basic Cards
- `bs4Card` - Standard content card/box (preferred over alias `box`)
  - Parameters: title, footer, status, solidHeader, width, height, collapsible, collapsed, closable, maximizable, elevation, headerBorder, label

- `updatebs4Card` - Update card properties (preferred over alias `updateCard`/`updateBox`)
  - Parameters: id, action (one of: "toggle", "remove", "restore", "maximize")

### Specialized Cards
- `bs4InfoBox` - Info box for displaying metrics (preferred over alias `infoBox`)
  - Parameters: title, value, subtitle, icon, color, href, width, elevation, fill, iconElevation, gradient

- `bs4ValueBox` - Value box for displaying key metrics (preferred over alias `valueBox`)
  - Parameters: value, subtitle, icon, href, width, color, elevation, footer

- `bs4SocialCard` - Social media style card (preferred over alias `socialBox`)

- `bs4UserCard` - User profile card (preferred over alias `userBox`)

### Card Components 
- `bs4CardSidebar` - Sidebar within a card (preferred over alias `boxSidebar`/`cardSidebar`)
  - Parameters: id, width, background, headerBorder, overlay, collapsed

- `cardDropdown` - Dropdown menu for cards
  - Parameters: ...

- `cardDropdownproduct` - product within a card dropdown
  - Parameters: text, icon, url, inputId

- `bs4CardLabel` - Label for cards (preferred over alias `boxLabel`/`cardLabel`)
  - Parameters: text, status

## UI Components

### Dropdown Components
- `bs4DropdownMenu` - Dropdown menu (preferred over alias `dropdownMenu`)
  - Parameters: type, badgeStatus, icon, headerText, .list, href, ...

- `notificationproduct` - Notification product in dropdown menu
  - Parameters: text, icon, status, href, inputId

- `messageproduct` - Message product in dropdown menu
  - Parameters: from, message, icon, time, href, image, color, inputId

- `taskproduct` - Task product in dropdown menu
  - Parameters: text, value, color, href, inputId

- `dropdownDivider` - Divider in dropdown menu

- `dropdownHeader` - Header in dropdown menu
  - Parameters: title

### User Components
- `bs4UserMenu` - User menu (preferred over alias `dashboardUser`)
  - Parameters: name, image, title, subtitle, footer, fluidRow, ...

- `userMessage` - User message component
  - Parameters: author, type, image, status, date, title, ...

### Indicators & Feedback
- `bs4Badge` - Badge component (preferred over alias `dashboardBadge`)
  - Parameters: text, status, rounded, pill

- `bs4ProgressBar` - Progress bar (preferred over alias `progressBar`)
  - Parameters: value, striped, animated, status, height, label

- `bs4MultiProgressBar` - Multiple progress bars (preferred over alias `multiProgressBar`)
  - Parameters: ...

- `bs4Alert` - Alert component (alias: `createAlert`)
  - Parameters: title, status, closable, width, elevation, content

- `bs4Callout` - Callout component (preferred over alias `callout`)
  - Parameters: title, status, elevation, width, content

### Layout Components
- `bs4Accordion` - Accordion component (preferred over alias `accordion`)
  - Parameters: id, ...

- `bs4Accordionproduct` - Accordion product (preferred over alias `accordionproduct`)
  - Parameters: title, collapsed, id, color, ...

- `bs4Timeline` - Timeline component (preferred over alias `timelineBlock`)
  - Parameters: ...

- `bs4Carousel` - Carousel component (preferred over alias `carousel`)
  - Parameters: id, width, height, status

- `bs4ListGroup` - List group component (preferred over alias `listGroup`)
  - Parameters: ...

## Utility Components

- `bs4Jumbotron` - Jumbotron/hero component (preferred over alias `jumbotron`)
  - Parameters: title, lead, href, status, btnName, btnStatus

- `bs4Ribbon` - Ribbon component (preferred over alias `ribbon`)
  - Parameters: text, color, position, size, textColor

- `bs4Stars` - Star rating component (preferred over alias `starBlock`)
  - Parameters: grade, max, size, color

- `bs4Sortable` - Sortable component (preferred over alias `sortable`)
  - Parameters: ...

## Interactivity Helpers

- `updatebs4Sidebar` - Toggle sidebar (preferred over alias `updateSidebar`)
  - Parameters: id, session

- `updatebs4Tabproducts` - Update active tab (preferred over alias `updateTabproducts`)
  - Parameters: session, inputId, selected

- `updateNavbarTabs` - Update active navbar tab
  - Parameters: session, inputId, selected

- `updatebs4CardSidebar` - Update card sidebar (preferred over alias `updateCardSidebar`/`updateBoxSidebar`)
  - Parameters: id, mode (one of: "toggle", "open", "close"), session

## Special Notes

1. When using bs4Dash, prefer the dedicated `bs4*` prefixed functions over their aliases:
   - Use `bs4DashPage` instead of `dashboardPage`
   - Use `bs4DashNavbar` instead of `dashboardHeader`
   - Use `bs4DashSidebar` instead of `dashboardSidebar`
   - Use `bs4Card` instead of `box`

2. There is NO `bs4Notificationproduct` function - use `notificationproduct` directly.

3. All functions should use proper namespacing:
   - `bs4Dash::bs4DashPage`
   - `bs4Dash::bs4Card`
   - `shiny::icon`
   - `shiny::tags$hr`

4. The newer bs4Dash version uses updated function names that better reflect their purpose:
   - `bs4DashNavbar` (not `dashboardHeader`)
   - `bs4DashSidebar` (not `dashboardSidebar`)
   - `bs4Card` (not `box`)
   - `bs4ValueBox` (not `valueBox`)