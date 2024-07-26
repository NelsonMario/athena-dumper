from builder.query import SimpleQueryBuilder

class FooQueryGenerator(SimpleQueryBuilder):
    def __init__(self, name=[], user_ids=[]):
        self.attributes = {
            "name": name,
            "user_id": user_ids
        }
        
    def get_users(self):
        return SimpleQueryBuilder("database.user").select().where(
            "name IN (%s)", self.attributes["name"]
        ).build()
        
    def get_transactions(self):
        return SimpleQueryBuilder("database.transaction").select().where(
            "user_id IN (%s)", self.attributes["user_id"]
        ).build()


