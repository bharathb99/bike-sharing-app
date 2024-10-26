import pika
import json
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://user:password@user_db:5432/userdb'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

    def to_dict(self):
        return {"id": self.id, "username": self.username, "email": self.email}

def publish_message(message):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', 5672))  # Connect to AMQP port
        channel = connection.channel()
        channel.queue_declare(queue='notifications', durable=True)
        
        channel.basic_publish(
            exchange='',
            routing_key='notifications',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        connection.close()
    except Exception as e:
        app.logger.error(f"Failed to publish message: {e}")

@app.route('/users/register', methods=['POST'])
def register_user():
    data = request.json
    new_user = User(username=data['username'], email=data['email'])
    db.session.add(new_user)
    db.session.commit()
    
    # Send notification
    publish_message({
        "type": "user_registered",
        "username": new_user.username,
        "email": new_user.email
    })
    
    return jsonify({"message": "User registered successfully!", "user": new_user.to_dict()}), 201

@app.route('/users/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    user = User.query.get(user_id)
    if user:
        db.session.delete(user)
        db.session.commit()
        
        # Send notification
        publish_message({
            "type": "user_deleted",
            "username": user.username,
            "email": user.email
        })
        
        return jsonify({"message": "User deleted successfully!"}), 200
    return jsonify({"message": "User not found!"}), 404

# Get user by ID
@app.route('/users/<int:user_id>', methods=['GET'])
def get_user(user_id):
    user = User.query.get(user_id)
    if user:
        return jsonify(user.to_dict()), 200
    return jsonify({"message": "User not found!"}), 404

# Get all users
@app.route('/users', methods=['GET'])
def get_all_users():
    users = User.query.all()
    return jsonify([user.to_dict() for user in users]), 200

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(host='0.0.0.0', port=5000)
