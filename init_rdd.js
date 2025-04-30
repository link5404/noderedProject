/**
 * Node-RED Node for initializing Spark RDDs
 */
module.exports = function(RED) {
    function InitRDDNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;

        // Store configuration
        node.rddName = config.rddName;
        node.payload = config.payload;
        node.payloadType = config.payloadType;

        // Function to send the message
        node.sendMessage = function(inputMsg = {}) {
            try {
                const rddName = inputMsg.rddName || node.rddName;

                // Create the RDD object
                const msg = {
                    payload: {
                        "Rdd-Name": rddName,
                        "Methods": []
                    }
                };

                // Send the message
                node.send(msg);

                // Update node status
                node.status({ fill: "green", shape: "dot", text: "RDD initialized" });

            } catch (error) {
                node.status({ fill: "red", shape: "dot", text: "error" });
                node.error("Error initializing RDD: " + error.message);
            }
        };

        // Triggered by input wire
        node.on('input', function(msg) {
            node.sendMessage(msg);
        });

        // Clear node status on close
        node.on('close', function() {
            node.status({});
        });
    }

    // Register the node
    RED.nodes.registerType("init_rdd", InitRDDNode);

    // Register one global admin endpoint to handle all button presses
    RED.httpAdmin.post("/init_rdd/:id", RED.auth.needsPermission("init_rdd.write"), function(req, res) {
        const node = RED.nodes.getNode(req.params.id);
        if (node) {
            node.log("Button pressed for node " + node.id);
            node.sendMessage(); // no inputMsg, defaults to configured value
            res.sendStatus(200);
        } else {
            res.sendStatus(404);
        }
    });
};
