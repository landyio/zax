# Schema

``` yaml

## Campaigns

instances:
  id:       _Number_ # Campaign instance \#ID
  runState: [ 'Loading', 'NoData', 'Training', 'Predicting', 'Suspended' ]
  config:
    variations: [ (_Number_) * ] # Variations \#IDs

```