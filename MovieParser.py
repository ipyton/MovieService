import json
def parseMovie(movie):

    result = {}
    result['resource_id'] = movie.resource_id
    result['actressList'] = list( [] if movie.actor_list is None else movie.actor_list )
    result['genre_list'] = list( [] if movie.genre_list is None else movie.genre_list )
    result['introduction'] = movie.introduction
    result['level']=movie.level
    result['makerList']=list( [] if movie.maker_list is None else movie.maker_list )
    result['movie_name']=movie.movie_name
    result['pictureList']=list( [] if movie.picture_list is None else movie.picture_list )
    result['poster']=movie.poster
    result['release_year']=movie.release_year
    result['score']=movie.score
    result['total_season']=movie.total_season
    result['type']=movie.type

    if movie.tags is not None:
        result['tags']=movie.tags
    return result