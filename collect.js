module.exports = function(RED) {
    function CollectNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        
        // Store collect parameters from node configuration
        this.collectTable = config.collectTable;
        
        node.on('input', function(msg) {

            if (msg.payload && typeof msg.payload === 'object') {
                var collectMethod = {
                    "type": "collect",
                    "table": this.collectTable
                };

                // Add collect method to Methods array
                msg.payload.Methods.push(collectMethod);

                // Send the modified message
                node.send(msg);

            } else {
                node.error("Payload must be a valid JSON object", msg);
            }
        });
    }
    
    RED.nodes.registerType("spark-collect", CollectNode, {
        defaults: {
            name: { value: "" },
            collectTable: { value: "", required: true }
        },
        category: "spark",
        color: "#E6E0F8",
        icon: "collect.png",
        label: function() {
            return this.name || "spark-collect";
        },
        paletteLabel: "collect"
    });
}