# tap-forecast

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the Forecast API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from the [Forecast REST API](https://github.com/Forecast-it/API)
- Extracts the following resources from Forecast:
  - [Allocations](https://github.com/Forecast-it/API/blob/master/sections/allocations.md#allocations)
  - [Cards](https://github.com/Forecast-it/API/blob/master/sections/cards.md#cards)
  - [Clients](https://github.com/Forecast-it/API/blob/master/sections/clients.md#clients)
  - [Connected projects](https://github.com/Forecast-it/API/blob/master/sections/connected_projects.md#connected_projects)
  - [Expense items](https://github.com/Forecast-it/API/blob/master/sections/expense_items.md#expense_items)
  - [Holiday calendar entries](https://github.com/Forecast-it/API/blob/master/sections/holiday_calendar_entries.md#holiday_calendar_entries)
  - [Holiday calendar](https://github.com/Forecast-it/API/blob/master/sections/holiday_calendars.md#holiday_calendars)
  - [Labels](https://github.com/Forecast-it/API/blob/master/sections/labels.md#labels)
  - [Milestones](https://github.com/Forecast-it/API/blob/master/sections/milestones.md#milestones)
  - [Non project time](https://github.com/Forecast-it/API/blob/master/sections/non_project_time.md#non_project_time)
  - [Person cost periods](https://github.com/Forecast-it/API/blob/master/sections/person_cost_periods.md#person_cost_periods)
  - [Persons](https://github.com/Forecast-it/API/blob/master/sections/persons.md#persons)
  - [Project team](https://github.com/Forecast-it/API/blob/master/sections/project_team.md#project_team)
  - [projects](https://github.com/Forecast-it/API/blob/master/sections/projects.md#projects)
  - [Rate card rates](https://github.com/Forecast-it/API/blob/master/sections/rate_card_rates.md#rate_card_rates)
  - [Rate card](https://github.com/Forecast-it/API/blob/master/sections/rate_cards.md#rate_cards)
  - [Repeating cards](https://github.com/Forecast-it/API/blob/master/sections/repeating_cards.md#repeating_cards)
  - [Roles](https://github.com/Forecast-it/API/blob/master/sections/roles.md#roles)
  - [Sprints](https://github.com/Forecast-it/API/blob/master/sections/sprints.md#sprints)
  - [Sub tasks](https://github.com/Forecast-it/API/blob/master/sections/sub_tasks.md#sub_tasks)
  - [Time registrations](https://github.com/Forecast-it/API/blob/master/sections/time_registrations.md#time_registrations)
  - [Workflow columns](https://github.com/Forecast-it/API/blob/master/sections/workflow_columns.md#workflow_columns)
- Outputs the schema for each resource

## Quick start

1. Install

   We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > pip install .
    ```

2. Get your Forecast api key and create the config file

    Create a JSON file containing the api key (required), a start date (required), if you want to change the replication method default: 'INCREMENTAL' (optional)
    ```json
    { 
      "API_KEY": "your-api-key",
      "start_date": "2020-01-01T00:00:00Z",
      "rewrite_replication_method": "INCREMENTAL"
    }
    ```

3. Run the tap in discovery mode to get properties.json file

    ```bash
    tap-forecast --config config.json --discover > catalog.json
    ```

4. In the properties.json file, select the streams to sync

    Each stream in the properties.json file has a "schema" entry.  To select a stream to sync, add `"selected": true` to that stream's "schema" entry.  For example, to sync the projects stream:
    ```
    ...
    "tap_stream_id": "projects",
    "schema": {
      "selected": true,
      "properties": {
        "updated_at": {
          "format": "date-time",
          "type": [
            "null",
            "string"
          ]
        }
    ...
    ```

5. Run the application

    `tap-forecast` can be run with:

    ```bash
    tap-forecast --config config.json --catalog catalog.json
    ```

---