import pandas as pd
import datetime
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///movies.db'
app.config['UPLOAD_FOLDER'] = 'uploads'
db = SQLAlchemy(app)

# Movie model with release_date as Date type
class Movie(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255), nullable=False)
    original_title = db.Column(db.String(255), nullable=True)
    original_language = db.Column(db.String(50), nullable=True)
    overview = db.Column(db.Text, nullable=True)
    release_date = db.Column(db.Date, nullable=True)  
    budget = db.Column(db.BigInteger, nullable=True)
    homepage = db.Column(db.Text, nullable=True)
    revenue = db.Column(db.BigInteger, nullable=True)
    runtime = db.Column(db.Float, nullable=True)
    status = db.Column(db.String(50), nullable=True)
    vote_average = db.Column(db.Float, nullable=True)
    vote_count = db.Column(db.Integer, nullable=True)
    production_company_id = db.Column(db.Integer, nullable=True)
    genre_id = db.Column(db.Integer, nullable=True)
    languages = db.Column(db.Text, nullable=True)

with app.app_context():
    db.create_all()

@app.route('/upload', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({"error": "Invalid file type. Only CSV allowed"}), 400
    
    try:
        df = pd.read_csv(file, encoding='utf-8')
        
        required_columns = {
            "budget", "homepage", "original_language", "original_title", "overview", 
            "release_date", "revenue", "runtime", "status", "title", 
            "vote_average", "vote_count", "production_company_id", "genre_id", "languages"
        }
        
        if not required_columns.issubset(df.columns):
            return jsonify({"error": "Invalid CSV format"}), 400

        for column in ['budget', 'revenue', 'vote_count', 'production_company_id']:
            df[column] = df[column].apply(lambda x: None if pd.isna(x) else x)

        for _, row in df.iterrows():
            release_date = row['release_date']
            if isinstance(release_date, str):
                try:
                    release_date = datetime.datetime.strptime(release_date, "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    release_date = None
            else:
                release_date = None
            
            languages_list = row['languages']
            if isinstance(languages_list, str):
                languages = ', '.join(eval(languages_list)) if languages_list else ''
            else:
                languages = row["languages"]
            
            movie = Movie(
                budget=row['budget'],
                homepage=row['homepage'],
                original_language=row['original_language'],
                original_title=row['original_title'],
                overview=row['overview'],
                release_date=release_date,
                revenue=row['revenue'],
                runtime=row['runtime'],
                status=row['status'],
                title=row['title'],
                vote_average=row['vote_average'],
                vote_count=row['vote_count'],
                production_company_id=row['production_company_id'],
                genre_id=row['genre_id'],
                languages=languages
            )
            db.session.add(movie)
        
        db.session.commit()
        db.session.remove()
        return jsonify({"message": "File uploaded and data saved successfully"}), 201
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/movies', methods=['GET'])
def get_movies():
    try:
        page = request.args.get('page', default=1, type=int)
        per_page = request.args.get('per_page', default=10, type=int)
        year = request.args.get('year', type=int)
        language = request.args.get('language', type=str)
        sort_by = request.args.get('sort_by', default='release_date', type=str)
        sort_order = request.args.get('sort_order', default='asc', type=str)

        query = Movie.query

        if year:
            query = query.filter(Movie.release_date.like(f"%{year}%"))
        
        if language:
            query = query.filter(Movie.languages.like(f"%{language}%"))

        if sort_by:
            if sort_by == 'release_date':
                if sort_order == 'asc':
                    query = query.order_by(Movie.release_date.asc())
                else:
                    query = query.order_by(Movie.release_date.desc())
            elif sort_by == 'vote_average':
                if sort_order == 'asc':
                    query = query.order_by(Movie.vote_average.asc())
                else:
                    query = query.order_by(Movie.vote_average.desc())

        movies_paginated = query.paginate(page=page, per_page=per_page, error_out=False)


        movies = [{
            "title": movie.title,
            "original_title": movie.original_title,
            "original_language": movie.original_language,
            "overview": movie.overview,
            "release_date": movie.release_date,
            "budget": movie.budget,
            "homepage": movie.homepage,
            "revenue": movie.revenue,
            "runtime": movie.runtime,
            "status": movie.status,
            "vote_average": movie.vote_average,
            "vote_count": movie.vote_count,
            "production_company_id": movie.production_company_id,
            "genre_id": movie.genre_id,
            "languages": movie.languages
        } for movie in movies_paginated.items]

        response = {
            "movies": movies,
            "page": page,
            "per_page": per_page,
            "total": movies_paginated.total
        }

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)






