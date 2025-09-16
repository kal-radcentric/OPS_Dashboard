using System.Windows;
using System.Windows.Controls;

namespace OPS_Dashboard
{
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
            DataContext = new MainViewModel();
        }

        private void txtDownloadLog_TextChanged(object sender, TextChangedEventArgs e)
        {
            // Auto-scroll to the bottom when new text is added
            if (scrollDownloadLog != null)
            {
                scrollDownloadLog.ScrollToEnd();
            }
        }

        private void txtProcessingLog_TextChanged(object sender, TextChangedEventArgs e)
        {
            // Auto-scroll to the bottom when new text is added
            if (scrollProcessingLog != null)
            {
                scrollProcessingLog.ScrollToEnd();
            }
        }
    }
}