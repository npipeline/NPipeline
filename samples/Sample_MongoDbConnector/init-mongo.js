// MongoDB initialization script
// Creates the 'shop' database, seeds 'orders' collection, and creates indexes

// Switch to the shop database
db = db.getSiblingDB('shop');

// Create the orders collection
db.createCollection('orders');

// Create indexes for better query performance
db.orders.createIndex({"status": 1});
db.orders.createIndex({"createdAt": 1});
db.orders.createIndex({"customer": 1});

// Seed sample orders
db.orders.insertMany([
    {
        _id: "order-001",
        customer: "Alice Johnson",
        amount: 150.00,
        status: "pending",
        createdAt: new Date("2024-01-15T10:30:00Z")
    },
    {
        _id: "order-002",
        customer: "Bob Smith",
        amount: 75.50,
        status: "pending",
        createdAt: new Date("2024-01-15T11:00:00Z")
    },
    {
        _id: "order-003",
        customer: "Carol Williams",
        amount: 200.00,
        status: "processing",
        createdAt: new Date("2024-01-15T11:30:00Z")
    },
    {
        _id: "order-004",
        customer: "David Brown",
        amount: 320.75,
        status: "pending",
        createdAt: new Date("2024-01-15T12:00:00Z")
    },
    {
        _id: "order-005",
        customer: "Eve Davis",
        amount: 95.25,
        status: "completed",
        createdAt: new Date("2024-01-14T09:00:00Z")
    }
]);

// Create the processed_orders collection (for ETL output)
db.createCollection('processed_orders');

print("MongoDB initialization complete!");
print("Database: shop");
print("Collections: orders, processed_orders");
print("Sample orders inserted: 5");
