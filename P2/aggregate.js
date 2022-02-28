use("alumno");

db.runCommand({
    aggregate: "restaurants",
    allowDiskUse: true,
    cursor: { batchSize: 40},
    pipeline: [
        {
            $match: {
                borough: "Manhattan", cuisine: "Spanish"
            }
        },
        {
            $group: {
                _id: "$address.zipcode",
                "restaurants1": {
                    $push: {
                        restaurant_id: "$restaurant_id",
                        name: "$name",
                        cuisine: "$cuisine",
                        address: "$address",
                        grades: "$grades"
                    }
                },
                "restaurants2": {
                    $push: {
                        restaurant_id: "$restaurant_id",
                        name: "$name",
                        cuisine: "$cuisine",
                        address: "$address",
                        grades: "$grades"
                    }
                }
            }
        },
        {
            $unwind: "$restaurants1"
        },
        {
            $unwind: "$restaurants2"
        },
        {
            $project: {
                _id: 0,
                zipcode: "$_id",
                restaurant1: {
                    id: "$restaurants1.restaurant_id",
                    name: "$restaurants1.name",
                    cuisine: "$restaurants1.cuisine",
                    address: "$restaurants1.address",
                },
                restaurant2: {
                    id: "$restaurants2.restaurant_id",
                    name: "$restaurants2.name",
                    cuisine: "$restaurants2.cuisine",
                    address: "$restaurants2.address",
                },
                distance: {
                    $sqrt: {
                        $sum: [
                            {
                                $pow: [
                                    {
                                        $subtract: [
                                            {$arrayElemAt: [ "$restaurants1.address.coord", 0 ]},
                                            {$arrayElemAt: [ "$restaurants2.address.coord", 0 ]}
                                        ]
                                    },
                                    2
                                ]
                            },
                            {
                                $pow: [
                                    {
                                        $subtract: [
                                            {$arrayElemAt: [ "$restaurants1.address.coord", 1 ]},
                                            {$arrayElemAt: [ "$restaurants2.address.coord", 1 ]}
                                        ]
                                    },
                                    2
                                ]
                            }
                        ]
                    }
                }
            }
        },
        {
            $redact: {
                $cond: {
                    if: {
                        $and: [
                            {$lt: ["$restaurant1.id", "$restaurant2.id"]},
                            {$ne: ["$distance", 0.0]}
                        ]
                    },
                    then: "$$KEEP",
                    else: "$$PRUNE"
                }
            }
        },
        {
            $group: {
                _id: "$zipcode",
                min_distance: {$min: "$distance"},
                restaurant_pairs: {
                    $push: {
                        restaurant1: "$restaurant1",
                        restaurant2: "$restaurant2",
                        distance: "$distance"
                    }
                }
            }
        },
        {
            $unwind: "$restaurant_pairs"
        },
        {
            $redact: {
                $cond: {
                    if: {
                        $eq: ["$restaurant_pairs.distance", "$min_distance"]
                    },
                    then: "$$KEEP",
                    else: "$$PRUNE"
                }
            }
        },
        {
            $project: {
                _id: 0,
                zipcode: "$_id",
                distance: "$restaurant_pairs.distance",
                restaurant1_name: "$restaurant_pairs.restaurant1.name",
                restaurant1_cuisine: "$restaurant_pairs.restaurant1.cuisine",
                restaurant1_address: "$restaurant_pairs.restaurant1.address.street",
                restaurant2_name: "$restaurant_pairs.restaurant2.name",
                restaurant2_cuisine: "$restaurant_pairs.restaurant2.cuisine",
                restaurant2_address: "$restaurant_pairs.restaurant2.address.street"
            }
        },
        {
            $sort: {
                zipcode: 1
            }
        },
        {
            $out: { 
                db: "alumno",
                coll: "rest_aggregate"
            }
        }
    ]
})