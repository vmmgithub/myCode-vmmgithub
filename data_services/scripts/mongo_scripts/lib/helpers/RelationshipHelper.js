var _ = require("underscore");

function RelationshipHelper() {

    return{

        replaceRelationship: function (model, relation) {
            var rels = _.reject(model.relationships, function (rel) {
                return rel.relation && rel.relation.name == relation.relation.name;
            });
            rels.push(relation);
            model.relationships = rels;
        },

        removeRelationships : function(model,relName){
            var rels = _.reject(model.relationships, function (rel) {
                return rel.relation && rel.relation.name == relName;
            });
            model.relationships = rels;
        },


        buildRelationship: function (relationLookup, target) {
            return {
                relation: {
                    type: relationLookup.type,
                    key: relationLookup._id,
                    name: relationLookup.name,
                    displayName: relationLookup.displayName
                },
                target: {
                    key: target._id,
                    displayName: target.displayName,
                    type: target.type
                }
            }
        },

        getRelationship: function (model, name) {
            var rels = [];
            _.each(model.relationships, function (rel) {
                if (rel.relation.name == name) {
                    rels.push(rel)
                }
            });
            return rels;
        }
    }
}

module.exports = RelationshipHelper;