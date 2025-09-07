# Theft Inference Model

## 📌 Project Overview
This project implements an **action-based theft detection model** that processes live video streams.  
The model continuously analyzes frames and raises an alert when suspicious activity is detected.

- **Input Sources**: Supports both **WebRTC** and **RTSP** video streams.
- **Frame Processing**: 
  - Takes sliding windows of frames:
    - 1–30, 2–31, 3–32, and so on.
  - Ensures temporal context for better action recognition.
- **Decision Logic**: 
  - If theft is detected **3 times out of 5 consecutive evaluations**,  
    an **alert "Theft Detected"** is triggered.
- **Output**: Real-time alerts for potential theft events.

---

## ⚙️ Workflow
1. Capture video stream from **WebRTC** or **RTSP**.
2. Extract **30-frame windows** for model input.
3. Run action recognition inference on each window.
4. Apply a **consecutive detection rule (3/5)**.
5. If rule is satisfied → **Send theft alert**.

---

## 🚀 Features
- Real-time video analysis.
- Supports both local and remote video streams.
- Sliding window
