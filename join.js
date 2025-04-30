/**
 * Node-RED Node for joining Spark RDDs into a nested structure
 */
module.exports = function(RED) {
    function RddJoinNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        
        // Store configuration
        this.joinedRddName = config.joinedRddName;
        this.joinExpression = config.joinExpression;
        
        // Track input messages for join
        var leftRDD = null;
        var rightRDD = null;
        
        // Process input messages
        this.on('input', function(msg, send, done) {
            send = send || function() { node.send.apply(node, arguments); };
            
            // Determine which input this is (0 or 1)
            var inputIdx = 0;
            if (msg._inputIdx !== undefined) {
                inputIdx = msg._inputIdx;
            }
            
            if (!leftRDD) {
                leftRDD = msg.payload;
                node.status({fill:"blue", shape:"dot", text:"left RDD received"});
            } else {
                rightRDD = msg.payload;
                node.status({fill:"blue", shape:"dot", text:"right RDD received"});
            }
            
            
            // If we have both RDDs, perform the join
            if (leftRDD && rightRDD) {
                try {
                    // Create the joined RDD structure
                    var joinedRDD = {
                        "Rdd-Name": node.joinedRddName,
                        "Joined-Rdds": [
                            leftRDD,
                            rightRDD
                        ],
                        "Methods": []
                    };
                    
                    // Create a simple join method with the expression
                    var joinMethod = {
                        "type": "join",
                        "expression":node.joinExpression
                    };
                    
                    // Add the join method to the Methods array
                    joinedRDD.Methods.push(joinMethod);
                    
                    // Create a new message with the joined RDD
                    var newMsg = {
                        topic: node.joinedRddName,
                        payload: joinedRDD
                    };
                    
                    // Send the message with the joined RDD
                    send(newMsg);
                    
                    // Reset for the next join
                    leftRDD = null;
                    rightRDD = null;
                    
                    node.status({fill:"green", shape:"dot", text:"joined"});
                    
                    // Clear status after a while
                    setTimeout(function() {
                        node.status({});
                    }, 3000);
                } catch (error) {
                    node.status({fill:"red", shape:"dot", text:"error"});
                    if (done) {
                        done(error);
                    } else {
                        node.error("Error joining RDDs: " + error.message);
                    }
                }
            }
            
            if (done) {
                done();
            }
        });
        
        // Clean up on close
        this.on('close', function() {
            leftRDD = null;
            rightRDD = null;
            node.status({});
        });
    }
    
    RED.nodes.registerType("rdd_join", RddJoinNode);
};