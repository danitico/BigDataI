db.runCommand({
    mapReduce: "restaurants",
    query: {borough: "Manhattan", cuisine: "Spanish"},
    map: function MapCode() {
        emit(
            this.address.zipcode,
            {
                "data": [
                    {
                        "name": this.name,
                        "cuisine": this.cuisine,
                        "address": this.address,
                        "grades": this.grades
                    }
                ]
            }
        );
    },
    reduce: function ReduceCode(key, values) {
        var reduced = {"data": []};
        for (var i in values) {
            var inter = values[i];
            for (var j in inter.data) {
                reduced.data.push(inter.data[j]);
            }
        }
        
        return reduced;
    },
    finalize: function Finalize(key, reduced) {
        if (reduced.data.length == 1) {
            return { "message" : "Este barrio solo contiene un restaurante" };
        }
        var min_dist = 999999999999;
        var city1 = {"address": {building: "", coord: [], "street": "", "zipcode": ""}};
        var city2 = {"address": {building: "", coord: [], "street": "", "zipcode": ""}};
        var c1, c2, d2, restaurants_lower_scores = 0;

        for (var i in reduced.data) {
            c1 = reduced.data[i];

            for (var grade_index in c1.grades) {
                if (c1.grades[grade_index].score <= 13) {
                    restaurants_lower_scores += 1;
                    break;
                }
            }
 
            for (var j in reduced.data) {
                if (i>=j) continue; //termina la iteración actual y continua con la siguiente j
                c2 = reduced.data[j];
                d2 = (c1.address.coord[0]-c2.address.coord[0])*(c1.address.coord[0]-c2.address.coord[0]) + (c1.address.coord[1]-c2.address.coord[1])*(c1.address.coord[1]-c2.address.coord[1]);
                if (d2 < min_dist && d2 > 0) {
                    min_dist = d2;
                    city1 = c1;
                    city2 = c2;
                }
            }
        }

        return {
            "restaurant1_name": city1.name,
            "restaurant1_cuisine": city1.cuisine,
            "restaurant1_address": city1.address.street,
            "restaurant2_name": city2.name,
            "restaurant2_cuisine": city2.cuisine,
            "restaurant2_address": city2.address.street,
            "dist": Math.sqrt(min_dist),
            "restaurants_with_lower_scores": restaurants_lower_scores
        };
    },
    out: { merge: "rest_mapreduce" }
})