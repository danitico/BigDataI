db.runCommand({
    mapReduce: "restaurants",
    query: {borough: "Manhattan", cuisine: "Spanish"},
    map: function MapCode() {
        emit(
            this.address.zipcode,
            {
                "name": this.name,
                "cuisine": this.cuisine,
                "address": this.address,
                "grades": this.grades
            }
        );
    },
    reduce: function ReduceCode(key, values) {
        var reduced = {"data": []};
        values.map(value => {
            reduced.data.push(value);
        });

        return reduced;
    },
    finalize: function Finalize(key, reduced) {
        if (reduced.data.length === 1) {
            return { "message" : "Este barrio solo contiene un restaurante" };
        }

        var minDistance = 999999999999;
        var bestRestaurant1, bestRestaurant2;
        var restaurantsWithLowerScores = 0;

        reduced.data.forEach((restaurant1, index) => {
            for (const grade of restaurant1.grades) {
                if (grade.score <= 13) {
                    restaurantsWithLowerScores += 1;
                    break;
                }
            }

            reduced.data.slice(index + 1).forEach(restaurant2 => {
                const distance = Math.pow(
                    restaurant1.address.coord[0] - restaurant2.address.coord[0],
                    2
                ) + Math.pow(
                    restaurant1.address.coord[1] - restaurant2.address.coord[1],
                    2
                );

                if (distance > 0 && distance < minDistance) {
                    bestRestaurant1 = restaurant1;
                    bestRestaurant2 = restaurant2;
                    minDistance = distance
                }
            })
        })

        return {
            "distance": Math.sqrt(minDistance),
            "restaurant1_name": bestRestaurant1.name,
            "restaurant1_cuisine": bestRestaurant1.cuisine,
            "restaurant1_address": bestRestaurant1.address.street,
            "restaurant2_name": bestRestaurant2.name,
            "restaurant2_cuisine": bestRestaurant2.cuisine,
            "restaurant2_address": bestRestaurant2.address.street,
            "restaurants_with_lower_scores": restaurantsWithLowerScores
        };
    },
    out: { merge: "rest_mapreduce" }
})
