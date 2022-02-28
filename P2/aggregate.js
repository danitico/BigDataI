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
            // Obtengo en un array los scores que son menores o iguales a 13
            // y lo guardo en el nuevo campo addFields
            $addFields: {
                lowScores: {
                    $filter: {
                        input: "$grades.score",
                        as: "num",
                        cond: {
                            $lte: ["$$num", 13]
                        }
                    }
                }
            }
        },
        {
            // A partir de ese array que he creado, si su tamaño es 0
            // solo tiene buenos scores y se asigna un 0 a la variable hasLowScores
            // Si su tamaño es distinto de 0 es porque tiene malos scores y se asigna
            // a la variable un 1.
            $addFields: {
                hasLowScores: {
                    $cond: [
                        {
                            $eq: [{$size: "$lowScores"}, 0]
                        },
                        0,
                        1
                    ]
                }
            }
        },
        {
            // En el group, se agrupa por código postal y se suma la variable hasLowScores
            // para finalmente darnos el numero de restaurantes que tienen malos scores en
            // ese código postal
            $group: {
                _id: "$address.zipcode",
                "restaurantsWithLowerScores": {
                    $sum: "$hasLowScores"
                },
                "restaurants1": {
                    $push: {
                        restaurant_id: "$restaurant_id",
                        name: "$name",
                        cuisine: "$cuisine",
                        address: "$address"
                    }
                },
                "restaurants2": {
                    $push: {
                        restaurant_id: "$restaurant_id",
                        name: "$name",
                        cuisine: "$cuisine",
                        address: "$address"
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
                restaurantsWithLowerScores: 1,
                restaurant1: {
                    id: "$restaurants1.restaurant_id",
                    name: "$restaurants1.name",
                    cuisine: "$restaurants1.cuisine",
                    address: "$restaurants1.address"
                },
                restaurant2: {
                    id: "$restaurants2.restaurant_id",
                    name: "$restaurants2.name",
                    cuisine: "$restaurants2.cuisine",
                    address: "$restaurants2.address"
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
                            {$gte: ["$restaurant1.id", "$restaurant2.id"]},
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
                        distance: "$distance",
                        restaurantsWithLowerScores: "$restaurantsWithLowerScores"
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
            // Se añade este paso para desempatar cuando hay dos pares de restaurantes
            // en un código postal que están a la misma distancia
            $group: {
                _id: "$_id",
                restaurant1: {$first: "$restaurant_pairs.restaurant1"},
                restaurant2: {$first: "$restaurant_pairs.restaurant2"},
                distance: {$first: "$restaurant_pairs.distance"},
                restaurantsWithLowerScores: {$first: "$restaurant_pairs.restaurantsWithLowerScores"}
            }
        },
        {
            $project: {
                _id: 0,
                zipcode: "$_id",
                distance: "$distance",
                restaurant1_name: "$restaurant1.name",
                restaurant1_cuisine: "$restaurant1.cuisine",
                restaurant1_address: "$restaurant1.address.street",
                restaurant2_name: "$restaurant2.name",
                restaurant2_cuisine: "$restaurant2.cuisine",
                restaurant2_address: "$restaurant2.address.street",
                restaurants_with_lower_scores: "$restaurantsWithLowerScores"
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
