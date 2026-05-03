#!/bin/bash
# 更新 app.R 的 Shell 腳本工具

# 顏色定義
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 函數：打印彩色訊息
print_success() { echo -e "${GREEN}✅ $1${NC}"; }
print_error() { echo -e "${RED}❌ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }

# 函數：顯示使用說明
show_usage() {
    echo "使用方法："
    echo "  $0                    # 互動式選擇"
    echo "  $0 <檔案名>           # 指定檔案"
    echo "  $0 --latest          # 更新到最新版本"
    echo "  $0 --version <數字>   # 更新到指定版本"
    echo ""
    echo "範例："
    echo "  $0 full_app_v17.R"
    echo "  $0 --latest"
    echo "  $0 --version 15"
}

# 函數：更新 app.R
update_app() {
    local source_file=$1
    
    # 檢查檔案是否存在
    if [ ! -f "$source_file" ]; then
        print_error "找不到檔案: $source_file"
        return 1
    fi
    
    # 檢查是否需要更新
    if [ -f "app.R" ]; then
        if diff -q app.R "$source_file" > /dev/null 2>&1; then
            print_info "app.R 已經與 $source_file 相同，無需更新"
            return 0
        fi
        
        # 備份現有檔案
        backup_name="app.R.backup.$(date +%Y%m%d_%H%M%S)"
        print_info "備份現有 app.R 為: $backup_name"
        cp app.R "$backup_name"
    fi
    
    # 執行更新
    print_info "複製 $source_file 到 app.R"
    if cp "$source_file" app.R; then
        print_success "app.R 已成功更新！"
        
        # 顯示檔案資訊
        echo ""
        echo "新的 app.R 資訊："
        echo "  - 大小: $(ls -lh app.R | awk '{print $5}')"
        echo "  - 來源: $source_file"
        echo ""
        echo "下一步："
        echo "1. 確認 manifest.json 是最新的：Rscript -e 'rsconnect::writeManifest()'"
        echo "2. 提交到 Git：git add app.R && git commit -m \"Update app.R from $source_file\""
        echo "3. 推送到 GitHub：git push"
        echo "4. 在 Posit Connect Cloud 重新部署"
        
        return 0
    else
        print_error "複製失敗！"
        return 1
    fi
}

# 主程式
echo ""
echo "📱 更新 app.R 工具"
echo "=================="
echo ""

# 處理參數
case "$1" in
    --help|-h)
        show_usage
        exit 0
        ;;
    --latest)
        update_app "full_app_v17.R"
        exit $?
        ;;
    --version)
        if [ -z "$2" ]; then
            print_error "請指定版本號碼"
            exit 1
        fi
        update_app "full_app_v$2.R"
        exit $?
        ;;
    "")
        # 互動式選擇
        echo "可用的應用程式檔案："
        echo "--------------------"
        
        # 列出所有 full_app_*.R 檔案
        i=1
        declare -a files
        for file in full_app_*.R; do
            if [ -f "$file" ]; then
                size=$(ls -lh "$file" | awk '{print $5}')
                date=$(ls -l "$file" | awk '{print $6, $7, $8}')
                echo "[$i] $file ($size, $date)"
                files[$i]=$file
                ((i++))
            fi
        done
        
        # 特別標記最新版本
        if [ -f "full_app_v17.R" ]; then
            echo ""
            echo "💡 建議使用 full_app_v17.R (最新版本)"
        fi
        
        # 讓用戶選擇
        echo ""
        read -p "請輸入編號選擇檔案 (輸入 0 取消): " choice
        
        if [ "$choice" = "0" ] || [ -z "$choice" ]; then
            print_info "取消操作"
            exit 0
        fi
        
        if [ -n "${files[$choice]}" ]; then
            update_app "${files[$choice]}"
        else
            print_error "無效的選擇"
            exit 1
        fi
        ;;
    *)
        # 直接指定檔案
        update_app "$1"
        exit $?
        ;;
esac 