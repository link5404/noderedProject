module.exports = function(RED) {
    function mapNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        
        // Store map parameters from node configuration
        this.mapExpression = config.mapExpression;
        
        node.on('input', function(msg) {
            if (msg.payload && typeof msg.payload === 'object') {
                var mapMethod = {
                    "type": "map",
                    "expression": this.mapExpression
                };
                
                // Add map method to Methods array
                msg.payload.Methods.push(mapMethod);
                
                // Send the modified message
                node.send(msg);
            } else {
                node.error("Payload must be a valid JSON object", msg);
            }
        });
    }
    
    RED.nodes.registerType("spark-map", mapNode, {
        defaults: {
            name: { value: "" },
            mapExpression: { value: "" }
        },
        category: "spark",
        color: "#E6E0F8",
        icon: "map.png",
        label: function() {
            return this.name || "spark-map";
        },
        paletteLabel: "map"
    });
}