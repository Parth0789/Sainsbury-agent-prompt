from model.model import Users


def fetch_user_details(db, email):
    result = db.query(
        Users
    ).filter(
        Users.email == email
    ).first()

    return result


def create_user_in_db(db, email, name, role):
    new_user = Users(
        name=name,
        email=email,
        company="sainsbury",
        roles=role,
        password="$2b$12$eUfZOR/29VuLbGDFC2/RNOCqtH8dv4wMLUkwwomK5Yva4x50IGtEa",
    )

    db.add(new_user)
    db.commit()

    return True