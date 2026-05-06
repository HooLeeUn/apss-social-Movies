from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVectorField
from django.db import migrations


SEARCH_VECTOR_EXPRESSION = """
    setweight(to_tsvector('simple', unaccent(coalesce(NEW.title_spanish, ''))), 'A') ||
    setweight(to_tsvector('simple', unaccent(coalesce(NEW.title_english, ''))), 'A') ||
    setweight(to_tsvector('simple', unaccent(coalesce(NEW.release_year::text, ''))), 'A') ||
    setweight(to_tsvector('simple', unaccent(coalesce(NEW.director, ''))), 'B') ||
    setweight(to_tsvector('simple', unaccent(coalesce(NEW.genre, ''))), 'C') ||
    setweight(to_tsvector('simple', unaccent(coalesce(NEW.cast_members, ''))), 'C') ||
    setweight(to_tsvector('simple', unaccent(coalesce(NEW.type, ''))), 'D')
"""

BACKFILL_SEARCH_VECTOR_EXPRESSION = """
    setweight(to_tsvector('simple', unaccent(coalesce(title_spanish, ''))), 'A') ||
    setweight(to_tsvector('simple', unaccent(coalesce(title_english, ''))), 'A') ||
    setweight(to_tsvector('simple', unaccent(coalesce(release_year::text, ''))), 'A') ||
    setweight(to_tsvector('simple', unaccent(coalesce(director, ''))), 'B') ||
    setweight(to_tsvector('simple', unaccent(coalesce(genre, ''))), 'C') ||
    setweight(to_tsvector('simple', unaccent(coalesce(cast_members, ''))), 'C') ||
    setweight(to_tsvector('simple', unaccent(coalesce(type, ''))), 'D')
"""


class Migration(migrations.Migration):

    atomic = False

    dependencies = [
        ("core", "0039_movie_accent_insensitive_autocomplete"),
    ]

    operations = [
        migrations.AddField(
            model_name="movie",
            name="search_vector",
            field=SearchVectorField(null=True),
        ),
        migrations.RunSQL(
            sql=f"""
                CREATE OR REPLACE FUNCTION core_movie_search_vector_update()
                RETURNS trigger AS $$
                BEGIN
                    NEW.search_vector :=
{SEARCH_VECTOR_EXPRESSION};
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS core_movie_search_vector_update_trigger ON core_movie;
                CREATE TRIGGER core_movie_search_vector_update_trigger
                BEFORE INSERT OR UPDATE OF
                    title_spanish,
                    title_english,
                    release_year,
                    director,
                    genre,
                    cast_members,
                    type
                ON core_movie
                FOR EACH ROW
                EXECUTE FUNCTION core_movie_search_vector_update();
            """,
            reverse_sql="""
                DROP TRIGGER IF EXISTS core_movie_search_vector_update_trigger ON core_movie;
                DROP FUNCTION IF EXISTS core_movie_search_vector_update();
            """,
        ),
        migrations.RunSQL(
            sql=f"""
                UPDATE core_movie
                SET search_vector =
{BACKFILL_SEARCH_VECTOR_EXPRESSION};
            """,
            reverse_sql="UPDATE core_movie SET search_vector = NULL;",
        ),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(fields=["search_vector"], name="movie_search_vector_gin_idx"),
        ),
    ]
