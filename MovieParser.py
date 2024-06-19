import json
def parseMovie(movie):

    result = {}
    result['movieId'] = movie.movieid
    result['actressList'] = [ x for x in movie.actress_list]
    result['genre_list'] = [ x for x in movie.genre_list]
    result['introduction'] = movie.introduction
    result['level']=movie.level
    result['makerList']=[x for x in movie.maker_list]
    result['movie_name']=movie.movie_name
    result['pictureList']=[x for x in movie.picture_list]
    result['poster']=movie.poster
    result['release_year']=movie.release_year
    result['score']=movie.score

    if movie.tags is not None:
        result['tags']=movie.tags
    return result