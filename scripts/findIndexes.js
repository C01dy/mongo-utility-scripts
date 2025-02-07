#!/usr/bin/env bun
import { MongoClient } from "mongodb";
import fs from "fs/promises";
import { config as dotenvConfig } from "dotenv";

// Загружаем переменные окружения из файла config/.env
dotenvConfig({ path: "config/.env" });

(async () => {
  // Чтение параметров подключения из переменных окружения
  const uri = process.env.DB_URI || "mongodb://localhost:27017";
  const dbName = process.env.DB_NAME || "your_database_name";

  const client = new MongoClient(uri);

  try {
    await client.connect();
    const db = client.db(dbName);

    // Получаем список всех коллекций
    const collections = await db.listCollections().toArray();

    // Папка для сохранения результатов поиска индексов
    const outputDir = "output/indexes_json";
    await fs.mkdir(outputDir, { recursive: true });

    // Обрабатываем каждую коллекцию
    for (const collInfo of collections) {
      const collectionName = collInfo.name;
      console.log(`Обрабатывается коллекция: ${collectionName}`);

      const collection = db.collection(collectionName);
      let indexes = [];

      try {
        // Получаем список индексов коллекции
        indexes = await collection.indexes();
      } catch (error) {
        console.error(`Ошибка при получении индексов для коллекции "${collectionName}": ${error.message}`);
        continue;
      }

      const indexCount = indexes.length;
      if (indexCount >= 64) {
        console.log(`Коллекция "${collectionName}" имеет ${indexCount} индексов.`);

        let indexUsageStats = [];
        try {
          // Получаем статистику использования индексов
          indexUsageStats = await collection.aggregate([{ $indexStats: {} }]).toArray();
        } catch (err) {
          console.error(
            `Ошибка при получении статистики использования индексов для коллекции "${collectionName}": ${err.message}`
          );
        }

        // Формируем карту: имя индекса → количество использований
        const usageMap = {};
        for (const stat of indexUsageStats) {
          usageMap[stat.name] = (stat.accesses && stat.accesses.ops) || 0;
        }

        // Добавляем каждому индексу поле usageCount и сортируем по убыванию usageCount
        const indexesWithUsage = indexes
          .map(index => ({
            ...index,
            usageCount: usageMap[index.name] || 0
          }))
          .sort((a, b) => b.usageCount - a.usageCount);

        const output = {
          collection: collectionName,
          indexCount,
          indexes: indexesWithUsage
        };

        const filename = `${outputDir}/${collectionName}_indexes.json`;
        try {
          await fs.writeFile(filename, JSON.stringify(output, null, 2), "utf8");
          console.log(`Результаты для коллекции "${collectionName}" записаны в файл: ${filename}`);
        } catch (err) {
          console.error(`Ошибка записи файла для коллекции "${collectionName}": ${err.message}`);
        }
      } else {
        console.log(`Коллекция "${collectionName}" имеет только ${indexCount} индексов (пропускаем).`);
      }
    }
  } catch (error) {
    console.error("Ошибка при выполнении запроса:", error);
  } finally {
    await client.close();
  }
})();
