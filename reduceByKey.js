module.exports = function(RED) {
    function ReduceByKeyNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        
        // Store reduce by key parameters from node configuration
        this.reduceByKeyExpression = config.reduceByKeyExpression;
        
        node.on('input', function(msg) {
            if (msg.payload && typeof msg.payload === 'object') {
                var reduceByKeyMethod = {
                    "type": "reduceByKey",
                    "expression": this.reduceByKeyExpression
                };
                
                // Add reduce by key method to Methods array
                msg.payload.Methods.push(reduceByKeyMethod);
                
                // Send the modified message
                node.send(msg);
            } else {
                node.error("Payload must be a valid JSON object", msg);
            }
        });
    }
    
    RED.nodes.registerType("spark-reduceByKey", ReduceByKeyNode, {
        defaults: {
            name: { value: "" },
            reduceByKeyExpression: { value: "" }
        },
        category: "spark",
        color: "#E6E0F8",
        icon: "reduceByKey.png",
        label: function() {
            return this.name || "spark-reduceByKey";
        },
        paletteLabel: "reduceByKey"
    });
}