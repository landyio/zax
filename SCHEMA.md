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
    _id:		_String_	\# Variation ID.
    name: 		_String_	\# Name
    active: 	_Boolean_   \# State of variation in the editor
	elements:   _Array_ 	\# List of elements
	campaign:   _ObjectId_ 	\# ID of parent campaign


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