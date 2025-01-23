# Transformar o DataFrame original em RDD
rdd1 = df1.rdd
rdd2 = df2.rdd

# Criar um dicionário a partir de df2 para consulta eficiente (join manual)
updates_dict = rdd2.map(lambda row: (row.id, row)).collectAsMap()

# Broadcast do dicionário para enviar uma única cópia para os executores
updates_broadcast = spark.sparkContext.broadcast(updates_dict)

# Definir uma função personalizada para aplicar as regras
def apply_rules(row):
    # Extrair os valores do dicionário de atualizações
    updates = updates_broadcast.value
    id = row.id
    status = row.status
    score = row.score
    category = row.category

    # Aplicar regras
    if id in updates:
        update = updates[id]
        # Atualizar status
        if update.new_status and update.is_active:
            status = update.new_status
        # Atualizar score
        if update.new_score and (update.new_score > score or status == "inactive"):
            score = update.new_score
        # Atualizar categoria
        if update.new_category:
            category = update.new_category

    return (id, status, score, category)

# Aplicar as regras no RDD usando map
updated_rdd = rdd1.map(apply_rules)

# Converter o RDD atualizado de volta para DataFrame
columns = ["id", "status", "score", "category"]
df_updated = updated_rdd.toDF(columns)

# Mostrar o resultado final
df_updated.show()
