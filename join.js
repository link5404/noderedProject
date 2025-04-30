module.exports = function(RED) {
    function joinNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        
        // Store join parameters from node configuration
        this.joinExpression = config.joinExpression;
        
        node.on('input', function(msg) {
            if (msg.payload && typeof msg.payload === 'object') {
                var joinMethod = {
                    "type": "join",
                    "expression": this.joinExpression
                };
                node.error(this.joinExpression)
                // Add join method to Methods array
                msg.payload.Methods.push(joinMethod);
                // Send the modified message
                node.send(msg);
            } else {
                node.error("Payload must be a valid JSON object", msg);
            }
        });
    }
    
    RED.nodes.registerType("spark-join", joinNode, {
        defaults: {
            name: { value: "" },
            joinExpression: { value: "" }
        },
        category: "spark",
        color: "#E6E0F8",
        icon: "join.png",
        label: function() {
            return this.name || "spark-join";
        },
        paletteLabel: "join"
    });
}