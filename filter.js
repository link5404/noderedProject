module.exports = function(RED) {
    function FilterNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        
        // Store filter parameters from node configuration
        this.filterExpression = config.filterExpression;
        this.tableName = config.tableName;
        
        node.on('input', function(msg) {
            if (msg.payload && typeof msg.payload === 'object') {
                var filterMethod = {
                    "type": "filter",
                    "expression": this.filterExpression,
                    "table": this.tableName
                };
                
                // Add filter method to Methods array
                msg.payload.Methods.push(filterMethod);
                
                // Send the modified message
                node.send(msg);
            } else {
                node.error("Payload must be a valid JSON object", msg);
            }
        });
    }
    
    RED.nodes.registerType("spark-filter", FilterNode, {
        defaults: {
            name: { value: "" },
            filterExpression: { value: "" , required: true},
            tableName: { value: "", required: true }
        },
        category: "spark",
        color: "#E6E0F8",
        icon: "filter.png",
        label: function() {
            return this.name || "spark-filter";
        },
        paletteLabel: "filter"
    })
}