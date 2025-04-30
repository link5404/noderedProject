/**
 * Node-RED Node for initializing Spark RDDs
 */
module.exports = function(RED) {
    function InitRDDNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        
        // Store configuration
        this.rddName = config.rddName;
        this.payload = config.payload;
        this.payloadType = config.payloadType;
        
        // Function to send the message
        this.sendMessage = function() {
            var msg = {};
            try {
                // Create the RDD object
                msg.payload = {
                    "Rdd-Name": node.rddName,
                    "Methods": []
                };
                
                // If a custom payload was provided, try to merge it
                if (node.payload && node.payloadType === 'json') {
                    try {
                        var customPayload = JSON.parse(node.payload);
                        if (customPayload && typeof customPayload === 'object') {
                            // Merge the custom payload with our defaults
                            for (var key in customPayload) {
                                if (customPayload.hasOwnProperty(key) && key !== "Rdd-Name" && key !== "Methods") {
                                    msg.payload[key] = customPayload[key];
                                }
                            }
                            
                            // If custom Methods are provided, use them
                            if (customPayload.Methods && Array.isArray(customPayload.Methods)) {
                                msg.payload.Methods = customPayload.Methods;
                            }
                        }
                    } catch (e) {
                        node.warn("Invalid JSON in payload configuration: " + e.message);
                    }
                }
                
                // Set the topic to the RDD name
                msg.topic = node.rddName;
                
                // Send the message
                node.send(msg);
                
                // Update node status
                node.status({fill:"green", shape:"dot", text:"RDD initialized"});
                
                // Clear status after a while
                setTimeout(function() {
                    node.status({});
                }, 3000);
            } catch (error) {
                node.status({fill:"red", shape:"dot", text:"error"});
                node.error("Error initializing RDD: " + error.message);
            }
        };
        
        // Handle button press
        RED.httpAdmin.post("/init_rdd/" + this.id, RED.auth.needsPermission("init_rdd.write"), function(req, res) {
            node.sendMessage();
            res.sendStatus(200);
        });
        
        // Also trigger on input (if there is any)
        this.on('input', function(msg) {
            // If an RDD name is provided in the incoming message, use it
            if (msg.rddName) {
                node.rddName = msg.rddName;
            }
            
            node.sendMessage();
        });
    }
    
    RED.nodes.registerType("init_rdd", InitRDDNode);
}