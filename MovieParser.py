import json
def parseMovie(movie):

    result = {}
    result['movieId'] = movie.movieid
    result['actressList'] = list( [] if movie.actress_list is None else movie.actress_list )
    result['genre_list'] = list( [] if movie.genre_list is None else movie.genre_list )
    result['introduction'] = movie.introduction
    result['level']=movie.level
    result['makerList']=list( [] if movie.maker_list is None else movie.maker_list )
    result['movie_name']=movie.movie_name
    result['pictureList']=list( [] if movie.picture_list is None else movie.picture_list )
    result['poster']=movie.poster
    result['release_year']=movie.release_year
    result['score']=movie.score

    if movie.tags is not None:
        result['tags']=movie.tags
    return result