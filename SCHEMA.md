# Schema

``` yaml

# Campaigns DB

landy:
  campaigns:
    ## ? ## 
    
  users:
    ## ? ##
  
  goals:
    ## ? ##
  
  metrics:
    ## ? ##
  
  variations:
    ## ? ##

  instances:
    id:       _Number_ # Campaign instance \#ID
    runState: [ 'Loading', 'NoData', 'Training', 'Predicting', 'Suspended' ]
    config:
      variations: [ (_Number_) * ] # Variations \#IDs
      
      
# Events DB

events:
  start:
    ## ? ##
    
  finish:
    ## ? ##
  

```