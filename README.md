ecaggregate
===========

elasticache configuration endpoint aggregator


why does this exist
===========

if you're using Elasticache in AWS, then you already know that you can't do multi-AZ deployments for a a single cache cluster.  you have to manually launch cache clusters in the zones you want to be in and then you probably take all of the node endpoints and put them in a single configuration file or something.  you've lost the best part besides managed cache: auto discovery.

what does this do
===========

ecaggregate takes all of your configuration endpoints, and constantly queries them to get the latest information all in one place.  the twist is that it then lets you listen on a custom port, or ports, and it can combine the responses from multiple configuration endpoints and serve them locally.

the simple example is that you're straddling three AZs, so you have three configuration endpoints.  you configure ecaggregate to query all three of them.  then, you configure ecaggregate to listen on localhost:11211, for example.  you configure that mapping (a mapping is a port to listen on and a list of configuration endpoints to combine the data from) to return the data from the three different configuration endpoints in a single response.  so, now, instead of having to query those three endpoints manually, you query the local one and it returns the combined node list of all three.  voila!
