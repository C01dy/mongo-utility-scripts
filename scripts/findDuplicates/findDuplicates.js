#!/usr/bin/env bun
import { MongoClient } from "mongodb";
import fs from "fs/promises";
import { config as dotenvConfig } from "dotenv";
import { generateConfigs } from "./utils";
import path from "path";

// Загружаем переменные окружения из файла input/.env
dotenvConfig(".env");

/**
 * Функция загрузки конфигурационного файла.
 * @param {string} filePath — путь к файлу конфигурации.
 * @returns {Promise<any>} — разобранный JSON.
 */
async function loadConfigs(filePath) {
  try {
    const content = await fs.readFile(filePath, "utf8");
    return JSON.parse(content);
  } catch (err) {
    throw new Error(`Ошибка чтения файла конфигурации "${filePath}": ${err.message}`);
  }
}

/**
 * Функция поиска дубликатов по заданным полям в указанных коллекциях.
 *
 * Для каждой конфигурации (содержащей имя коллекции и список полей)
 * производится агрегация, которая группирует документы по значению поля,
 * считает их количество и оставляет только группы с количеством документов > 1.
 *
 * Если имя коллекции содержит "rels", дополнительно выполняется запрос к коллекции
 * "meta.rels" для получения значений dest.cardinality и source.cardinality.
 *
 * Результат возвращается в виде массива объектов, например:
 * [
 *   {
 *     collection: "sharan.rels.clientContact",
 *     results: [ { field: "client", duplicates: [...] }, ... ],
 *     destCardinality: 10,
 *     sourceCardinality: 5
 *   },
 *   ...
 * ]
 */
async function findDuplicates(configs, { uri, dbName }) {
  const client = new MongoClient(uri);
  const finalResults = [];

  try {
    await client.connect();
    const db = client.db(dbName);

    // Обрабатываем каждую конфигурацию
    for (const config of configs) {
      const { collection: collectionName, fields } = config;
      if (!collectionName || !Array.isArray(fields)) {
        console.error("Некорректная конфигурация:", config);
        continue;
      }

      console.log(`\nОбработка коллекции: ${collectionName}`);
      const collection = db.collection(collectionName);
      const collectionResults = [];

      // Для каждого поля запускаем pipeline для поиска дубликатов
      for (const field of fields) {
        console.log(`Проверка дубликатов для поля: ${field}`);

        const pipeline = [
          {
            $group: {
              _id: `$${field}`, // Группировка по значению поля
              count: { $sum: 1 },
              docs: { $push: { _id: "$_id" } }
            }
          },
          { $match: { count: { $gt: 1 } } },
          { $sort: { count: -1 } }
        ];

        const duplicates = await collection.aggregate(pipeline).toArray();

        if (duplicates.length > 0) {
          console.log(`Найдены дубликаты для поля "${field}" в коллекции "${collectionName}"`);
          collectionResults.push({ field, duplicates });
        } else {
          console.log(`Дубликатов по полю "${field}" в коллекции "${collectionName}" не найдено.`);
        }
      }

      // Если для коллекции найдены дубликаты, формируем результирующий объект
      if (collectionResults.length > 0) {
        const resultObj = {
          collection: collectionName,
          results: collectionResults
        };

        // Если имя коллекции содержит "rels", выполняем дополнительный запрос в meta.rels
        if (collectionName.includes("rels")) {
          console.log(`Коллекция "${collectionName}" содержит "rels". Выполняем запрос к meta.rels для collectionName "${collectionName}".`);
          const metaRelationDoc = await db
            .collection("meta.rels")
            .findOne({ collectionType: collectionName });

          if (metaRelationDoc) {
            resultObj.destCardinality =
              metaRelationDoc.dest && metaRelationDoc.dest.cardinality;
            resultObj.sourceCardinality =
              metaRelationDoc.source && metaRelationDoc.source.cardinality;
          } else {
            console.log(`Документ в meta.rels с collectionType "${collectionName}" не найден.`);
          }
        }

        finalResults.push(resultObj);
      }
    }
  } catch (error) {
    console.error("Ошибка при выполнении запроса:", error);
  } finally {
    await client.close();
  }

  return finalResults;
}

(async () => {
  // Чтение параметров подключения из переменных окружения
  const uri = process.env.DB_URI || "mongodb://localhost:27017";
  const dbName = process.env.DB_NAME || "your_database_name";

  // Сначала запускаем генерацию файла конфигурации
  await generateConfigs();

  // Формируем путь к файлу конфигурации (duplicateKeys.json) в каталоге output/duplicates_json
  const configFile = path.resolve("output", "duplicates_json", "duplicateKeys.json");
  let configs = [];
  try {
    configs = await loadConfigs(configFile);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }

  // Поиск дубликатов
  const duplicatesResults = await findDuplicates(configs, { uri, dbName });
  console.log("\nИтоговые результаты поиска дубликатов:");
  console.log(JSON.stringify(duplicatesResults, null, 2));

  // Формируем путь к каталогу для сохранения результатов (output/duplicates_json)
  const outputDir = path.resolve("output", "duplicates_json");
  try {
    await fs.mkdir(outputDir, { recursive: true });
  } catch (err) {
    console.error(`Ошибка при создании папки "${outputDir}":`, err);
    process.exit(1);
  }

  // Записываем результаты для каждой коллекции в отдельный JSON-файл
  for (const resultObj of duplicatesResults) {
    const { collection } = resultObj;
    const filename = path.resolve(outputDir, `${collection}_duplicates.json`);
    try {
      await fs.writeFile(filename, JSON.stringify(resultObj, null, 2), "utf8");
      console.log(`Результаты для коллекции "${collection}" записаны в файл: ${filename}`);
    } catch (err) {
      console.error(`Ошибка записи файла для коллекции "${collection}":`, err);
    }
  }
})();
