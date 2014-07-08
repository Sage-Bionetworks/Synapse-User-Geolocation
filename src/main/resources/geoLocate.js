function geoLocate(divname, geoLocInfo) {
    map = new OpenLayers.Map(divname);
    map.addLayer(new OpenLayers.Layer.OSM());
   	var zoom=2;
 
    var markers = new OpenLayers.Layer.Markers( "Markers" );
    map.addLayer(markers);

    // from http://stackoverflow.com/questions/8523446/openlayers-simple-mouseover-on-marker
    var popup = null;

    for (i=0; i<geoLocInfo.length; i++) {
      	var lonLat = new OpenLayers.LonLat( geoLocInfo[i].latLng[1], geoLocInfo[i].latLng[0] )
          .transform(
            new OpenLayers.Projection("EPSG:4326"), // transform from WGS 1984
            map.getProjectionObject() // to Spherical Mercator Projection
          );
 
 
      	var size = new OpenLayers.Size(15,15);
      	var offset = new OpenLayers.Pixel(-(size.w/2), -size.h);
      	var icon = new OpenLayers.Icon('https://www.synapse.org/favicon.ico', size, offset);
      	var marker = new OpenLayers.Marker(lonLat,icon.clone());
      	marker.location = lonLat;
      	marker.locationString = geoLocInfo[i].location;
      	marker.userIds = geoLocInfo[i].userIds;

		var action = 'mousedown'; //'mouseover'
		marker.events.register(action, marker, function(evt) {
			// here we get the location, stored earlier
			var popupLocation = this.location;
			contentSize = new OpenLayers.Size(300,50+20*this.userIds.length);
			var html = '<div>'+this.locationString+'<br/><ul>';
    		for (i=0; i<this.userIds.length; i++) {
    			html = html + '<li><a href=https://www.synapse.org/#!Profile:'+this.userIds[i]+'>'+this.userIds[i]+'</a></li>';
    		}
    		html = html + '</ul>'+'</div>';
    		if (popup!=null) {
    			popup.hide();
    			popup=null;
    		}
    		popup = new OpenLayers.Popup(null, // ID
    			popupLocation,
    			contentSize,
    			html,
    			true,
    			null);
    		map.addPopup(popup);
		});
		
		//here add mouseout event
		//marker.events.register('mouseout', marker, function(evt) {popup.hide();});

      	markers.addMarker(marker);     
    }
 
    map.setCenter ([0,0], zoom);
}
