module.exports = function(RED) {
    const fs = require('fs');
    const path = require('path');
    
    // Function to get all CSV files from datasets folder
    function getDatasetFiles() {
        try {
            const datasetsPath = path.join(__dirname, 'datasets');
            if (fs.existsSync(datasetsPath)) {
                return fs.readdirSync(datasetsPath)
                    .filter(file => file.endsWith('.csv'))
                    .sort();
            }
            return [];
        } catch (err) {
            console.error("Error reading datasets directory:", err);
            return [];
        }
    }
    
    function TableSelectorNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        
        // Store selected tables from node configuration
        // Convert the string back to an array
        this.selectedTables = config.selectedTables || "[]";
        try {
            this.selectedTables = JSON.parse(this.selectedTables);
        } catch (e) {
            this.selectedTables = [];
            node.error("Error parsing selected tables");
        }
        
        node.on('input', function(msg) {
            if (msg.payload && typeof msg.payload === 'object') {
                // Ensure the Tables array exists
                if (!msg.payload.Tables) {
                    msg.payload.Tables = [];
                }
                
                // Set the Tables array to the selected tables
                msg.payload.Tables = [...this.selectedTables];
                
                // Send the modified message
                node.send(msg);
            } else {
                node.error("Payload must be a valid JSON object", msg);
            }
        });
    }
    
    RED.nodes.registerType("spark-table-selector", TableSelectorNode, {
        defaults: {
            name: { value: "" },
            selectedTables: { value: "[]", required: true } // Stored as JSON string for compatibility
        },
        category: "spark",
        color: "#E6E0F8",
        icon: "db.png",
        label: function() {
            return this.name || "spark-table-selector";
        },
        paletteLabel: "table selector"
    });
    
    // Expose the list of available datasets to the editor
    RED.httpAdmin.get('/spark-table-selector/datasets', RED.auth.needsPermission('spark-table-selector.read'), function(req, res) {
        res.json(getDatasetFiles());
    });
}