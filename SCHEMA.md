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
    _id:			_String_	\# Variation ID.
    name: 			_String_	\# Name
    active: 		_Boolean_   \# State of variation in the editor
	elements:   	_Object_ 	\# List of elements
		selector: 	_String_	\# CSS Selector
		name: 		_String_	\# Name of element
		html:		_String_	\# Html diff if changed
		attrs:		_Object_	\# CSS attributtes diff if changed
		styles:		_Object_	\# CSS styles diff if changed
		defStyles	_Object_	\# Original element styles
		defAttrs: 	_Object_	\# Original element attributes
		defHtml:	_String_	\# Original element content
	url:			_String_	\# Url for split testing
	campaign:   	_ObjectId_ 	\# ID of parent campaign


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