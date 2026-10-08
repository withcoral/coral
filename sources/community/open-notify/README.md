# Open Notify (ISS)

Real-time data about the International Space Station and astronauts in space from [Open Notify](http://open-notify.org/Open-Notify-API/).

## Setup

No authentication is required. Add the source:

```bash
coral source add --file sources/community/open-notify/manifest.yaml
```

## Local Testing

```bash
coral sql "
  SELECT name, craft 
  FROM open_notify.astronauts 
  LIMIT 5
"

/*
+----------------------+-------+
| name                 | craft |
+----------------------+-------+
| Oleg Kononenko       | ISS   |
| Nikolai Chub         | ISS   |
| Tracy Caldwell Dyson | ISS   |
| Matthew Dominick     | ISS   |
| Michael Barratt      | ISS   |
+----------------------+-------+
*/
```

## Tables

| Table | Description |
|-------|-------------|
| `iss_position` | Current geographic location of the International Space Station. |
| `astronauts` | List of astronauts currently in space. |
