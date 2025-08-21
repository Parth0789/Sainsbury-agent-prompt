from crud.users import fetch_user_details, create_user_in_db


class UsersService:
    def __init__(self, db, email, password=None):
        self.db = db
        self.email = email
        self.password = password

    def get_user_details(self):
        user = fetch_user_details(self.db, self.email)

        return user

    def create_user(self, email, role):
        name = email.split('@')[0]
        try:
            result = create_user_in_db(self.db, email, name, role)
        except Exception as e:
            print(e)
            raise e

        return result


    @classmethod
    def set_user_roles_permission(cls, role) -> dict:
        permissions = {
            "admin": False,
            'stores': False,
            'region': False,
            'area': False,
            'viewer': False,
            "analytics": False,
            "security": False,
            "support": False
        }

        if role in permissions:
            permissions[role] = True

        return permissions
