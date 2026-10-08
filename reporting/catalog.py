USER_REPORTS = {"users": "Usuarios registrados", "countries": "Usuarios por país", "ages": "Usuarios por rango de edad", "genders": "Usuarios por identidad de género"}
CONTENT_REPORTS = {"productions": "Producciones más y mejor calificadas", "genres": "Géneros más y mejor calificados", "combinations": "Combinaciones de géneros más y mejor calificadas", "recommended": "Producciones más recomendadas"}
DIRECT = {"ratings": "Calificaciones realizadas", "comments": "Comentarios públicos realizados", "videos": "Video reacciones publicadas", "added": "Producciones añadidas a Recomendadas", "removed": "Producciones retiradas de Recomendadas"}
SOCIAL = {"comment_likes": "Likes a comentarios públicos", "comment_dislikes": "Dislikes a comentarios públicos", "video_likes": "Likes a video reacciones", "video_dislikes": "Dislikes a video reacciones", "follows": "Follows realizados"}
REPORTS = {**USER_REPORTS, **CONTENT_REPORTS, "direct": "Actividad directa consolidada", **DIRECT, "social": "Actividad indirecta consolidada", **SOCIAL}
AGE_RANGES = {"13_17": (13, 17), "18_30": (18, 30), "31_40": (31, 40), "41_50": (41, 50), "51_plus": (51, None)}
AGE_LABELS = {"13_17": "13–17", "18_30": "18–30", "31_40": "31–40", "41_50": "41–50", "51_plus": "Más de 50"}
MONTH_NAMES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
