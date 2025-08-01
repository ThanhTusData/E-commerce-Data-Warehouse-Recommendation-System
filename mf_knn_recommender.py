import pandas as pd
import numpy as np
import os
from datetime import datetime
from sklearn.preprocessing import LabelEncoder
import joblib
import logging
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from collections import defaultdict
from scipy.sparse import csr_matrix
from sklearn.preprocessing import normalize
from concurrent.futures import ThreadPoolExecutor
import time

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='model_training.log')

class MF_KNN_Recommender:
    
    def __init__(self, model_dir="models", latent_factors=150, steps=500, k=5, alpha=0.001, beta=0.01):
        self.model_dir = model_dir
        self.user_encoder = LabelEncoder()
        self.item_encoder = LabelEncoder()
        self.user_factors = None
        self.item_factors = None
        self.k = k
        self.latent_factors = latent_factors
        self.steps = steps
        self.alpha = alpha
        self.beta = beta
        os.makedirs(model_dir, exist_ok=True)
        
        # Performance metrics
        self.training_time = 0
        self.last_train_size = 0

    def load_data(self, df=None, csv_path=None):
        try:
            start_time = time.time()

            if df is not None:
                # Use provided DataFrame
                input_df = df.copy()
            elif csv_path is not None:
                # Load from CSV
                input_df = pd.read_csv(csv_path)
            else:
                raise ValueError("Either df or csv_path must be provided")

            # Check for required columns
            if 'customer_key' not in input_df.columns or 'product_key' not in input_df.columns:
                raise ValueError("DataFrame must contain 'customer_key' and 'product_key' columns")

            # Drop missing values and duplicates
            input_df = input_df[['customer_key', 'product_key']].dropna().drop_duplicates()

            # Convert to correct and memory-efficient data types
            input_df['customer_key'] = input_df['customer_key'].astype(str).astype('category')
            input_df['product_key'] = input_df['product_key'].astype(str).astype('category')

            load_time = time.time() - start_time
            logging.info(f"Loaded {len(input_df)} records in {load_time:.2f} seconds")
            return input_df

        except Exception as e:
            logging.error(f"Error loading data: {e}")
            raise


    def encode_data(self, df):
        self.user_encoder.fit(df["customer_key"])
        self.item_encoder.fit(df["product_key"])
        
        encoded_df = df.copy()
        encoded_df["customer_key"] = self.user_encoder.transform(df["customer_key"])
        encoded_df["product_key"] = self.item_encoder.transform(df["product_key"])
        return encoded_df

    def create_interaction_matrix(self, df):
        if len(df["customer_key"].unique()) == 0 or len(df["product_key"].unique()) == 0:
            raise ValueError("No users or items found after encoding")
            
        # More efficient way to create interaction matrix
        num_users = len(self.user_encoder.classes_)
        num_items = len(self.item_encoder.classes_)
        
        # Use sparse matrix construction from COO format
        rows = df["customer_key"].values
        cols = df["product_key"].values
        data = np.ones(len(rows))
        
        interaction = csr_matrix((data, (rows, cols)), shape=(num_users, num_items))
        return interaction

    def matrix_factorization(self, R, k, steps, alpha, beta):
        num_users, num_items = R.shape
        if num_users <= 0 or num_items <= 0:
            raise ValueError(f"Invalid matrix dimensions: {num_users}x{num_items}")

        # Use sparse representation for better memory usage
        if not isinstance(R, csr_matrix):
            R = csr_matrix(R)
            
        # Initialize factors with small random values
        np.random.seed(42)  # For reproducibility
        P = np.random.normal(0, 0.01, (num_users, k))
        Q = np.random.normal(0, 0.01, (num_items, k))
        
        # Get non-zero indices for faster iteration
        non_zero = np.array(R.nonzero()).T
        
        # Adaptive learning rate
        adaptive_alpha = alpha
        
        # Early stopping
        best_error = float('inf')
        patience = 5
        no_improvement = 0
        
        for step in range(steps):
            np.random.shuffle(non_zero)
            
            # Mini-batch processing
            batch_size = min(1000, len(non_zero))
            total_error = 0
            
            for i in range(0, len(non_zero), batch_size):
                batch = non_zero[i:i+batch_size]
                
                # Vectorized update for batch
                batch_i, batch_j = batch[:, 0], batch[:, 1]
                
                # Calculate errors
                pred = np.sum(P[batch_i] * Q[batch_j], axis=1)
                actual = np.array([R[i, j] for i, j in batch])
                errors = actual - pred
                
                # Update factors
                for idx in range(len(batch)):
                    i, j = batch_i[idx], batch_j[idx]
                    eij = errors[idx]
                    
                    P[i] += adaptive_alpha * (2 * eij * Q[j] - beta * P[i])
                    Q[j] += adaptive_alpha * (2 * eij * P[i] - beta * Q[j])
                
                # Prevent numerical instability
                P = np.clip(P, -10, 10)
                Q = np.clip(Q, -10, 10)
                
                # Accumulate error
                total_error += np.sum(errors ** 2)
            
            # Reduce learning rate over time
            adaptive_alpha = alpha * (1.0 - step/steps)
            
            # Early stopping check
            if total_error < best_error:
                best_error = total_error
                no_improvement = 0
            else:
                no_improvement += 1
                
            if no_improvement >= patience:
                logging.info(f"Early stopping at step {step}/{steps}")
                break
                
            if step % 50 == 0:
                logging.info(f"Training step {step}/{steps}, Error: {total_error:.4f}")
                
        return P, Q

    def train(self, df=None, csv_path=None):
        try:
            start_time = time.time()
            
            processed_df = self.load_data(df=df, csv_path=csv_path)
            self.last_train_size = len(processed_df)
            
            # Ensure there's data to work with
            if processed_df.empty:
                raise ValueError("Dataset is empty")
            
            logging.info(f"Starting encoding for {len(processed_df)} records")
            encoded_df = self.encode_data(processed_df)
            
            logging.info("Creating interaction matrix")
            interaction_matrix = self.create_interaction_matrix(encoded_df)
            
            # Print dimensions before factorization
            num_users, num_items = interaction_matrix.shape
            logging.info(f"Interaction matrix dimensions: {num_users}x{num_items}")
            
            logging.info(f"Starting matrix factorization with {self.latent_factors} factors")
            self.user_factors, self.item_factors = self.matrix_factorization(
                interaction_matrix, self.latent_factors, self.steps, self.alpha, self.beta)

            model_data = {
                "user_factors": self.user_factors,
                "item_factors": self.item_factors,
                "user_encoder": self.user_encoder,
                "item_encoder": self.item_encoder,
                "training_metadata": {
                    "training_date": datetime.now().isoformat(),
                    "num_users": num_users,
                    "num_items": num_items,
                    "latent_factors": self.latent_factors,
                    "steps": self.steps
                }
            }
            
            model_path = os.path.join(self.model_dir, "mf_knn_model.pkl")
            joblib.dump(model_data, model_path)
            
            self.training_time = time.time() - start_time
            logging.info(f"MF-KNN model trained and saved in {self.training_time:.2f} seconds.")
            
            return {
                "status": "success",
                "training_time": self.training_time,
                "num_users": num_users,
                "num_items": num_items,
                "model_path": model_path
            }
        except Exception as e:
            logging.error(f"Error in train method: {e}")
            raise

    def get_user_index(self, user_id):
        """Helper to get user index from user ID"""
        if isinstance(user_id, str):
            if user_id not in self.user_encoder.classes_:
                logging.warning(f"User {user_id} not found in trained data")
                return None
            return self.user_encoder.transform([user_id])[0]
        else:
            if user_id < 0 or user_id >= len(self.user_factors):
                logging.error(f"User index {user_id} out of bounds (0-{len(self.user_factors)-1})")
                return None
            return user_id

    def recommend_items_for_user(self, customer_key, top_k=5, show_scores=False):
        try:
            # Add validation to ensure user factors and item factors are loaded
            if self.user_factors is None or self.item_factors is None:
                raise ValueError("Model not trained. Call train() first.")
            
            user_idx = self.get_user_index(customer_key)
            if user_idx is None:
                msg = f"Khách hàng '{user_id}' chưa có lịch sử mua hàng trong dữ liệu huấn luyện."
                logging.warning(msg)
                return msg
                
            user_vector = self.user_factors[user_idx].reshape(1, -1)
            # Normalize only if the vector is not all zeros
            if np.any(user_vector):
                user_vector = normalize(user_vector)[0]
            else:
                user_vector = user_vector[0]
                
            # Precompute normalized item matrix
            item_matrix = self.item_factors
            if np.any(item_matrix):
                item_matrix = normalize(item_matrix)
                
            # Calculate scores
            scores = np.dot(item_matrix, user_vector)
            
            # Sort and get top indices
            top_k = min(top_k, len(scores))
            top_indices = np.argsort(scores)[::-1][:top_k]

            # Output product keys and scores
            recommended_items = self.item_encoder.inverse_transform(top_indices)
            item_scores = scores[top_indices]
            
            # Return a list of products and their corresponding scores
            recommendations = list(zip(recommended_items, item_scores))
            
            # Only print if show_scores is True
            if show_scores:
                for item, score in recommendations:
                    print(f"Product Key: {item}, Score: {score:.4f}")
            
            return recommended_items
        except Exception as e:
            logging.error(f"Error in recommend_items_for_user: {e}")
            return []

    def batch_recommend(self, customer_keys, top_k=5, use_threads=False):
        if self.user_factors is None or self.item_factors is None:
            raise ValueError("Model not trained. Call train() first.")
            
        results = {}
        
        if use_threads and len(customer_keys) > 10:  # Only use threads for larger batches
            try:
                with ThreadPoolExecutor(max_workers=min(4, len(customer_keys))) as executor:
                    future_to_user = {executor.submit(self.recommend_items_for_user, customer_key, top_k, False): customer_key for customer_key in customer_keys}
                    for future in future_to_user:
                        customer_key = future_to_user[future]
                        try:
                            results[customer_key] = future.result(timeout=10)  # Add timeout to prevent hanging
                        except Exception as e:
                            logging.error(f"Error recommending for customer {customer_key}: {e}")
                            results[customer_key] = []
            except KeyboardInterrupt:
                logging.warning("Batch recommendation interrupted by user")
                # Return partial results that we've collected so far
                return results
        else:
            # Sequential processing
            for customer_key in customer_keys:
                try:
                    results[customer_key] = self.recommend_items_for_user(customer_key, top_k, False)
                except Exception as e:
                    logging.error(f"Error recommending for customer {customer_key}: {e}")
                    results[customer_key] = []
                    
        return results

    def plot_item_embeddings(self, top_n=100, save_path="product_embeddings.png"):
        try:
            # Validate item factors exist
            if self.item_factors is None:
                raise ValueError("Model not trained. Call train() first.")
                
            pca = PCA(n_components=2)
            item_2d = pca.fit_transform(self.item_factors)
            item_labels = self.item_encoder.classes_

            plt.figure(figsize=(15, 10))
            top_n = min(top_n, len(item_labels))
            
            # Create a scatter plot with better visibility
            scatter = plt.scatter(item_2d[:top_n, 0], item_2d[:top_n, 1], 
                                  s=80, alpha=0.7, c=np.arange(top_n), cmap='viridis')

            # Add labels with better positioning and visibility
            for i in range(top_n):
                plt.annotate(item_labels[i], 
                             (item_2d[i, 0], item_2d[i, 1]),
                             fontsize=9,
                             alpha=0.8,
                             xytext=(5, 2),
                             textcoords='offset points',
                             bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.7))

            plt.title(f"Product Embedding Space (top {top_n} products)", fontsize=14)
            plt.xlabel("Component 1", fontsize=12)
            plt.ylabel("Component 2", fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(save_path, dpi=300)
            plt.close()
            logging.info(f"Product embedding plot saved as {save_path}")
            
            return save_path
        except Exception as e:
            logging.error(f"Error in plot_item_embeddings: {e}")
            return None

    def evaluate(self, test_df=None, test_csv_path=None, k=5):
        try:
            # Validate model is trained
            if self.user_factors is None or self.item_factors is None:
                raise ValueError("Model not trained. Call train() first.")
            
            # Load test data
            if test_df is not None:
                test_data = test_df.copy()
            elif test_csv_path is not None:
                test_data = pd.read_csv(test_csv_path)
            else:
                logging.warning("No test data provided. Cannot evaluate.")
                return {"precision": 0, "recall": 0, "f1_score": 0, "num_evaluated_users": 0}
                
            # Ensure we have the required columns
            if 'customer_key' not in test_data.columns or 'product_key' not in test_data.columns:
                raise ValueError("Test data must contain 'customer_key' and 'product_key' columns")
                
            # Select only required columns and convert to string
            test_data = test_data[['customer_key', 'product_key']]
            test_data['customer_key'] = test_data['customer_key'].astype(str)
            test_data['product_key'] = test_data['product_key'].astype(str)

            # Group by user for faster lookup
            actual_items_by_user = defaultdict(set)
            for _, row in test_data.iterrows():
                actual_items_by_user[row["customer_key"]].add(row["product_key"])

            # Only evaluate users that exist in the trained model
            common_users = set(actual_items_by_user.keys()) & set(self.user_encoder.classes_)
            
            if not common_users:
                logging.warning("No common users found between test data and trained model")
                return {"precision": 0, "recall": 0, "f1_score": 0, "num_evaluated_users": 0}
                
            precision_list = []
            recall_list = []
            
            # For large datasets, limit evaluation to a sample
            eval_users = list(common_users)
            if len(eval_users) > 1000:
                np.random.shuffle(eval_users)
                eval_users = eval_users[:1000]
                
            # Batch processing for efficiency
            batch_recs = self.batch_recommend(eval_users, top_k=k)
            
            for customer_key in eval_users:
                recs = batch_recs.get(customer_key, [])
                actual = actual_items_by_user[customer_key]
                
                if not recs or not actual:
                    continue
                    
                hits = set(recs) & actual
                precision = len(hits) / k if recs else 0
                recall = len(hits) / len(actual) if actual else 0
                
                precision_list.append(precision)
                recall_list.append(recall)

            avg_precision = np.mean(precision_list) if precision_list else 0
            avg_recall = np.mean(recall_list) if recall_list else 0
            
            f1_score = 2 * (avg_precision * avg_recall) / (avg_precision + avg_recall) if (avg_precision + avg_recall) > 0 else 0
            
            results = {
                "precision": avg_precision,
                "recall": avg_recall,
                "f1_score": f1_score,
                "num_evaluated_users": len(precision_list)
            }
            
            print(f"Evaluation results (@{k}):")
            print(f"Precision: {avg_precision:.4f}")
            print(f"Recall: {avg_recall:.4f}")
            print(f"F1 Score: {f1_score:.4f}")
            print(f"Evaluated on {len(precision_list)} users")
            
            return results
        except Exception as e:
            logging.error(f"Error in evaluate: {e}")
            return {"precision": 0, "recall": 0, "f1_score": 0, "num_evaluated_users": 0}

    def coverage(self, sample_size=100):
        try:
            if self.user_factors is None or self.item_factors is None:
                raise ValueError("Model not trained. Call train() first.")
                
            # Sample users for efficiency - limit to a reasonable number to avoid performance issues
            sample_size = min(sample_size, len(self.user_encoder.classes_))
            np.random.seed(42)  # For reproducibility
            sample_users = np.random.choice(self.user_encoder.classes_, size=sample_size, replace=False)
            
            unique_items = set()
            total_items = len(self.item_encoder.classes_)

            # Use sequential processing to avoid threading issues
            batch_recs = self.batch_recommend(sample_users, top_k=self.k, use_threads=False)
            
            for customer_key, recs in batch_recs.items():
                unique_items.update(recs)

            coverage_score = len(unique_items) / total_items if total_items > 0 else 0
            
            print(f"Coverage: {coverage_score:.4f} ({len(unique_items)} out of {total_items} items)")
            return coverage_score
        except Exception as e:
            logging.error(f"Error in coverage: {e}")
            return 0

    def diversify_recommendations(self, customer_key, top_k=5, lambda_diversity=0.5):
        try:
            # Validate model is trained
            if self.user_factors is None or self.item_factors is None:
                raise ValueError("Model not trained. Call train() first.")
                
            user_idx = self.get_user_index(customer_key)
            if user_idx is None:
                return []

            user_vector = self.user_factors[user_idx].reshape(1, -1)
            # Normalize only if the vector is not all zeros
            if np.any(user_vector):
                user_vector = normalize(user_vector)[0]
            else:
                user_vector = user_vector[0]
                
            item_matrix = self.item_factors
            # Normalize only if the matrix is not all zeros
            if np.any(item_matrix):
                item_matrix = normalize(item_matrix)
                
            # Calculate relevance scores
            relevance_scores = np.dot(item_matrix, user_vector)

            # Get candidate pool (3x the desired recommendations)
            top_k_candidates = min(top_k * 3, len(relevance_scores))
            top_indices = np.argsort(relevance_scores)[::-1][:top_k_candidates]
            
            # Compute pairwise similarity matrix for candidates
            candidates_matrix = item_matrix[top_indices]
            similarity_matrix = np.dot(candidates_matrix, candidates_matrix.T)
            
            # Maximal Marginal Relevance algorithm
            selected = []
            remaining = list(range(len(top_indices)))
            
            # Select the most relevant item first
            best_idx = np.argmax(relevance_scores[top_indices])
            selected.append(remaining.pop(best_idx))
            
            # Select the rest using MMR
            while len(selected) < top_k and remaining:
                mmr_scores = []
                
                for i in remaining:
                    # Relevance term
                    rel_score = relevance_scores[top_indices[i]]
                    
                    # Diversity term - maximum similarity to items already selected
                    if selected:
                        sim_score = np.max([similarity_matrix[i, j] for j in selected])
                    else:
                        sim_score = 0
                        
                    # MMR score = λ * rel - (1-λ) * sim
                    mmr = lambda_diversity * rel_score - (1-lambda_diversity) * sim_score
                    mmr_scores.append(mmr)
                
                # Get item with highest MMR score
                next_idx = np.argmax(mmr_scores)
                selected.append(remaining.pop(next_idx))
            
            # Get the actual item indices and names
            diverse_indices = [top_indices[i] for i in selected]
            diverse_items = self.item_encoder.inverse_transform(diverse_indices)
            
            return diverse_items
        except Exception as e:
            logging.error(f"Error in diversify_recommendations: {e}")
            return []
            
    def load_model(self, model_path=None):
        if model_path is None:
            model_path = os.path.join(self.model_dir, "mf_knn_model.pkl")
            
        try:
            if not os.path.exists(model_path):
                logging.error(f"Model file not found: {model_path}")
                return False
                
            start_time = time.time()
            model_data = joblib.load(model_path)
            
            self.user_factors = model_data["user_factors"]
            self.item_factors = model_data["item_factors"]
            self.user_encoder = model_data["user_encoder"]
            self.item_encoder = model_data["item_encoder"]
            
            # Load metadata if available
            if "training_metadata" in model_data:
                metadata = model_data["training_metadata"]
                logging.info(f"Model trained on {metadata.get('training_date')} with {metadata.get('latent_factors')} factors")
                
            load_time = time.time() - start_time
            logging.info(f"Model loaded successfully from {model_path} in {load_time:.2f} seconds")
            return True
        except Exception as e:
            logging.error(f"Error loading model: {e}")
            return False
    
    def get_model_info(self):
        """Get information about the loaded model"""
        if self.user_factors is None or self.item_factors is None:
            return {"status": "Not loaded"}
            
        return {
            "status": "Loaded",
            "num_users": len(self.user_encoder.classes_),
            "num_items": len(self.item_encoder.classes_),
            "latent_factors": self.latent_factors,
            "training_time": self.training_time,
            "last_train_size": self.last_train_size
        }


# Example usage
if __name__ == "__main__":
    # Khởi tạo recommender
    recommender = MF_KNN_Recommender(
        latent_factors=100,  # Giảm xuống để đào tạo nhanh hơn
        steps=200,           # Giảm xuống để đào tạo nhanh hơn
        k=5,
        alpha=0.001,
        beta=0.01
    )
    
    # Tải mô hình hoặc đào tạo mô hình mới
    try:
        if recommender.load_model():
            print("Mô hình đã được tải thành công!")
        else:
            raise FileNotFoundError("Không tìm thấy mô hình")
    except Exception as e:
        print(f"Đang đào tạo mô hình mới... ({str(e)})")
        recommender.train('segment_dataset.csv')
    
    # Ví dụ cho các user cụ thể
    test_users = [
        '00bad631eaa8723983a6c7e2f910dfca', 
        '0277bd798b42071cb092b1795dbae947', 
        '00788705370da606e9f8390258b9e977'
    ]
    
    print("\n========== KHUYẾN NGHỊ CHO CÁC USER CỤ THỂ ==========")
    for i, user_id in enumerate(test_users):
        print(f"\n{i+1}. Khuyến nghị cho user {user_id}:")
        try:
            recs = recommender.recommend_items_for_user(user_id, top_k=5, show_scores=True)
        except Exception as e:
            print(f"Lỗi khi tạo khuyến nghị: {e}")
    
    print("\n========== KHUYẾN NGHỊ HÀNG LOẠT ==========")
    batch_results = recommender.batch_recommend(test_users, top_k=3, use_threads=False)
    for i, (user_id, recs) in enumerate(batch_results.items()):
        print(f"{i+1}. User {user_id}: {', '.join(recs)}")
    
    print("\n========== KHUYẾN NGHỊ ĐA DẠNG HÓA ==========")
    for i, user_id in enumerate(test_users):
        print(f"\n{i+1}. Khuyến nghị đa dạng hóa cho user {user_id}:")
        try:
            diverse_recs = recommender.diversify_recommendations(user_id, top_k=5, lambda_diversity=0.7)
            print(f"   {', '.join(diverse_recs)}")
        except Exception as e:
            print(f"   Lỗi: {e}")
    
    # Tạo biểu đồ trực quan hóa embedding sản phẩm
    print("\n========== ĐANG TẠO BIỂU ĐỒ EMBEDDING ==========")
    plot_path = recommender.plot_item_embeddings(top_n=30, save_path="product_embeddings.png")
    if plot_path:
        print(f"Biểu đồ embedding đã được lưu tại: {plot_path}")
    
    # Tính độ bao phủ (sử dụng mẫu nhỏ hơn)
    print("\n========== ĐỘ BAO PHỦ KHUYẾN NGHỊ ==========")
    coverage = recommender.coverage(sample_size=50) 
    
    # Đánh giá hiệu suất mô hình
    print("\n========== ĐÁNH GIÁ HIỆU SUẤT MÔ HÌNH ==========")
    evaluation = recommender.evaluate(k=5)